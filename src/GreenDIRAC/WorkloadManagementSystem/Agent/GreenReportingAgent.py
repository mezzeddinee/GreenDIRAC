#!/usr/bin/env python3
"""
GreenReportingAgent — Queries GreenDIGIT CIM APIs for each site
and records per-job green metrics (PUE, CI, Energy, Emissions)
into DIRAC JobDB or ElasticSearch.
"""
import pprint
import time
import requests
from datetime import datetime, timedelta, timezone

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary

JOB_PARAMETER_KEYS = [
    "ModelName", "CPUNormalizationFactor", "HostName", "JobID", "JobType",
    "LoadAverage", "MemoryUsed(kb)", "NormCPUTime(s)", "ScaledCPUTime(s)",
    "Status", "TotalCPUTime(s)", "WallClockTime(s)", "DiskSpace(MB)",
    "CEQueue", "GridCE",
]

JOB_ATTRIBUTE_KEYS = [
    "JobGroup", "JobName", "Owner", "OwnerDN", "OwnerGroup", "RescheduleCounter",
    "Site", "SubmissionTime", "StartExecTime", "EndExecTime",
    "SystemPriority", "UserPriority",
]

TIME_STAMPS = ["SubmissionTime", "StartExecTime", "EndExecTime"]

# Defaults
DEFAULT_CI = 24.0
DEFAULT_PUE = 1.5
DEFAULT_TDP = 150
ENERGY_WH_DEFAULT = 8500


class GreenReportingAgent(AgentModule):
    """Agent for collecting and reporting green computing metrics."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobDB = None
        self.elasticJobParametersDB = None
        self.maxJobsAtOnce = 50
        self.section = PathFinder.getAgentSection(self.agentName)

        # CIM API settings
        self.login = "atsareg@in2p3.fr"
        self.password = "Green@31415"
        self.token_max_age_hours = 24
        self.cim_api_base = "https://mc-a4.lab.uvalight.net/gd-ci-api"
        self.metrics_db_url = "https://mc-a4.lab.uvalight.net/gd-cim-api/submit"
        self.token = None
        self.token_ts = None

    # ----------------------------------------------------------------------
    def initialize(self):
        """Initialize agent: databases, config, CPU models, etc."""
        self.jobDB = JobDB()
        useES = Operations().getValue("/Services/JobMonitoring/useESForJobParametersFlag", False)
        if useES:
            try:
                result = ObjectLoader().loadObject(
                    "WorkloadManagementSystem.DB.ElasticJobParametersDB",
                    "ElasticJobParametersDB",
                )
                if not result["OK"]:
                    return result
                self.elasticJobParametersDB = result["Value"](parentLogger=self.log)
            except RuntimeError as e:
                return S_ERROR(f"Can't connect to ES DB: {e}")

        self.maxJobsAtOnce = self.am_getOption("MaxJobsAtOnce", self.maxJobsAtOnce)
        self.login = self.am_getOption("CIM_EMAIL", self.login)
        self.password = self.am_getOption("CIM_PASSWORD", self.password)
        self.cim_api_base = self.am_getOption("cim_api_base", self.cim_api_base)
        self.metrics_db_url = self.am_getOption("metrics_db_url", self.metrics_db_url)

        # Load CPU models (TDP, cores)
        self.cpuDict = {}
        result = gConfig.getSections(f"{self.section}/CPUData")
        if result["OK"]:
            for model in result["Value"]:
                self.cpuDict[model] = {
                    "TDP": gConfig.getValue(f"{self.section}/CPUData/{model}/TDP", DEFAULT_TDP),
                    "Cores": gConfig.getValue(f"{self.section}/CPUData/{model}/Cores", 12),
                }

        self.log.info(f"Loaded {len(self.cpuDict)} CPU models from config")
        return S_OK()

    # ----------------------------------------------------------------------
    def execute(self):
        """Main agent execution loop."""
        condDict = {"Status": [JobStatus.DONE, JobStatus.FAILED], "ApplicationNumStatus": 0}
        result = self.jobDB.selectJobs(condDict, limit=self.maxJobsAtOnce, orderAttribute="LastUpdateTime:DESC")
        if not result["OK"]:
            return result

        jobList = result["Value"]
        if not jobList:
            self.log.info("No jobs to process.")
            return S_OK()

        self.log.info(f"Loaded {len(jobList)} jobs for processing (max {self.maxJobsAtOnce})")

        # Retrieve parameters and attributes
        params = self.elasticJobParametersDB.getJobParameters(jobList)
        attrs = self.jobDB.getJobsAttributes(jobList)
        if not params["OK"] or not attrs["OK"]:
            return S_ERROR("Failed to load job parameters or attributes")

        jobParamsDict, jobAttrDict = params["Value"], attrs["Value"]
        records = []
        for job in jobParamsDict:
            record = {}
            for k, v in jobParamsDict[job].items():
                if k in JOB_PARAMETER_KEYS:
                    record[k] = v
            for k, v in jobAttrDict[job].items():
                if k in JOB_ATTRIBUTE_KEYS:
                    record[k] = str(v) if k in TIME_STAMPS else v
            records.append(record)

        # Mark processed
        self.jobDB.setJobAttributes(jobList, ["ApplicationNumStatus"], [9999])

        # Process and enrich
        for rec in records:
            cpuRes = self.__getProcessorParameters(rec.get("ModelName", "Unknown"))
            if not cpuRes["OK"]:
                continue
            tdp, cores = cpuRes["Value"]

            site = rec.get("Site", "Unknown")
            siteRes = self.__getSiteParameters(site)
            if not siteRes["OK"]:
                continue
            pue, ci, gocdb = siteRes["Value"]

            cpu_s = float(rec.get("TotalCPUTime(s)", 0))
            energy_kwh = cpu_s * tdp / cores / 1000.0 / 3600.0
            emissions = energy_kwh * pue * ci / 1000.0  # gCO2

            rec.update({
                "Energy(kwh)": energy_kwh,
                "TDP": tdp,
                "NCores": cores,
                "PUE": pue,
                "CI": ci,
                "CO2(g)": emissions,
                "SiteName": gocdb,
                "VO": Registry.getVOForGroup(rec.get("OwnerGroup")),
                "GreenTimestamp": datetime.utcnow().isoformat() + "Z",
            })

            self.log.info(
                f"✅ JobID={rec.get('JobID')} | Energy={energy_kwh:.6f} kWh | "
                f"PUE={pue:.2f} | CI={ci:.2f} | Emissions={emissions:.4f} gCO₂"
            )
            self.__sendRecordToMB(rec)
            self.__storeJobGreenMetrics(rec)

        return S_OK()

    # ----------------------------------------------------------------------
    def __get_jwt_token(self):
        """Authenticate with CIM API and cache the JWT token."""
        if self.token and self.token_ts:
            age = (time.time() - float(self.token_ts)) / 3600
            if age < self.token_max_age_hours:
                return self.token
        url = f"{self.cim_api_base.rstrip('/')}/../gd-cim-api/get-token"
        r = requests.post(url, json={"email": self.login, "password": self.password}, timeout=10)
        r.raise_for_status()
        self.token = r.json().get("access_token")
        self.token_ts = time.time()
        self.log.info("🔐 Refreshed CIM JWT token")
        return self.token

    # ----------------------------------------------------------------------
    def __getSiteParameters(self, site):
        """Query the CIM APIs to get site-level PUE and CI dynamically."""
        try:
            # 1. DIRAC → GOCDB
            result = getDIRACGOCDictionary()
            if not result["OK"]:
                gocdb_name = site
            else:
                gocdb_name = result["Value"].get(site, site)

            headers = {
                "Authorization": f"Bearer {self.__get_jwt_token()}",
                "Content-Type": "application/json",
            }

            # 2. /pue
            pue_url = f"{self.cim_api_base.rstrip('/')}/pue"
            pue_resp = requests.post(pue_url, headers=headers, json={"site_name": gocdb_name}, timeout=10)
            if pue_resp.status_code != 200:
                self.log.warn(f"PUE query failed for {gocdb_name} ({pue_resp.status_code})")
                return self.__getFallbackSiteParams(site, gocdb_name)

            pue_data = pue_resp.json()
            pue = float(pue_data.get("pue", DEFAULT_PUE))
            loc = pue_data.get("location", {})
            lat, lon = loc.get("latitude"), loc.get("longitude")
            if not lat or not lon:
                self.log.warn(f"No coordinates for {gocdb_name}")
                return self.__getFallbackSiteParams(site, gocdb_name, pue=pue)

            # 3. /ci with retries
            ci = None
            for offset_h in [5, 12, 24]:
                t = (datetime.now(timezone.utc) - timedelta(hours=offset_h)).isoformat(timespec="seconds").replace("+00:00", "Z")
                ci_payload = {
                    "lat": lat, "lon": lon, "pue": pue,
                    "energy_wh": ENERGY_WH_DEFAULT,
                    "time": t, "metric_id": gocdb_name,
                    "wattnet_params": {"granularity": "hour"},
                }
                ci_url = f"{self.cim_api_base.rstrip('/')}/ci"
                ci_resp = requests.post(ci_url, headers=headers, json=ci_payload, timeout=10)
                if ci_resp.status_code == 200:
                    ci_data = ci_resp.json()
                    ci_raw = ci_data.get("ci_gco2_per_kwh")
                    ci_eff = ci_data.get("effective_ci_gco2_per_kwh") or (ci_raw * pue if ci_raw else DEFAULT_CI)
                    ci = ci_eff
                    break
                elif ci_resp.status_code == 502:
                    continue

            if ci is None:
                ci = DEFAULT_CI

            self.log.info(f"🌍 Site={site} GOCDB={gocdb_name} → PUE={pue} CI={ci}")
            return S_OK((pue, ci, gocdb_name))

        except Exception as e:
            self.log.error(f"__getSiteParameters failed for {site}: {e}")
            return self.__getFallbackSiteParams(site, site)

    # ----------------------------------------------------------------------
    def __getFallbackSiteParams(self, site, gocdb_name, pue=None):
        """Fallback to static DIRAC config."""
        grid = site.split(".")[0]
        pue = pue or gConfig.getValue(f"/Resources/Sites/{grid}/{site}/GreenParams/PUE", DEFAULT_PUE)
        ci = gConfig.getValue(f"/Resources/Sites/{grid}/{site}/GreenParams/CI", DEFAULT_CI)
        return S_OK((pue, ci, gocdb_name))

    # ----------------------------------------------------------------------
    def __getProcessorParameters(self, model):
        """Get TDP and number of cores for a CPU model."""
        if model in self.cpuDict:
            return S_OK((self.cpuDict[model]["TDP"], self.cpuDict[model]["Cores"]))
        self.log.warn(f"Unknown CPU model: {model}")
        return S_OK((DEFAULT_TDP, 12))

    # ----------------------------------------------------------------------
    def __sendRecordToMB(self, record):
        """Send record to external metrics backend (CIM submit API)."""
        headers = {
            "Authorization": f"Bearer {self.__get_jwt_token()}",
            "Content-Type": "application/json",
        }
        resp = requests.post(self.metrics_db_url, headers=headers, json=record, timeout=15)
        self.log.info(f"Sent record to CIM API → status {resp.status_code}")
        return S_OK()

    # ----------------------------------------------------------------------

    def __storeJobGreenMetrics(self, record):
        """
        Store computed green metrics (Energy, PUE, CI, CO2, Timestamp)
        into the JobDB. Compatible with DIRAC 8.x.
        """
        jobID = record.get("JobID")
        if not jobID:
            self.log.error("Missing JobID in record, cannot store metrics.")
            return

        try:
            # Build the list of key/value tuples expected by JobDB
            params = [
                ("Energy(kwh)", record.get("Energy(kwh)")),
                ("PUE", record.get("PUE")),
                ("CI", record.get("CI")),
                ("CO2(g)", record.get("CO2(g)")),
                ("GreenTimestamp", record.get("GreenTimestamp")),
            ]

            # Call JobDB API
            result = self.jobDB.setJobParameters(jobID, params)

            # Check success
            if not result["OK"]:
                self.log.error(
                    f"❌ Failed to write JobDB record for JobID={jobID}: {result['Message']}"
                )
            else:
                self.log.info(
                    f"✅ Green metrics + emissions stored in JobDB for JobID={jobID}"
                )

        except Exception as e:
            self.log.error(f"Exception while writing record to JobDB: {e}")
