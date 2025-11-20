#!/usr/bin/env python3
"""
GreenReportingAgent — Queries GreenDIGIT CIM APIs for each site
and records per-job green metrics (PUE, CI, Energy, Emissions)
into DIRAC JobDB or ElasticSearch.
"""

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


# --------------------------------------------------------
# DIRAC job parameters and attributes
# --------------------------------------------------------
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


# ==========================================================
#               GreenReportingAgent
# ==========================================================
class GreenReportingAgent(AgentModule):
    """Agent for collecting and reporting green computing metrics."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.jobDB = None
        self.elasticJobParametersDB = None
        self.maxJobsAtOnce = 50

        self.section = PathFinder.getAgentSection(self.agentName)

        # CIM API settings
        self.login = None
        self.password = None
        self.cim_api_base = None
        self.metrics_db_url = None

        self.token = None
        self.token_ts = None
        self.token_max_age_hours = 24

    # -----------------------------------------------------
    def initialize(self):
        """Initialize agent: DBs, config, CPU models."""

        self.jobDB = JobDB()

        # Use ElasticSearch or JobDB params
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

        # Agent options
        self.maxJobsAtOnce = self.am_getOption("MaxJobsAtOnce", self.maxJobsAtOnce)

        self.login = self.am_getOption("CIM_EMAIL")
        self.password = self.am_getOption("CIM_PASSWORD")
        self.cim_api_base = self.am_getOption("CIM_API_BASE")
        self.metrics_db_url = self.am_getOption("CIM_METRICS_URL")

        if not self.login or not self.password:
            return S_ERROR("CIM_EMAIL and CIM_PASSWORD must be set in configuration")

        if not self.cim_api_base or not self.metrics_db_url:
            return S_ERROR("CIM_API_BASE and CIM_METRICS_URL must be set in configuration")

        # Load CPU models
        self.cpuDict = {}
        result = gConfig.getSections(f"{self.section}/CPUData")
        if result["OK"]:
            for model in result["Value"]:
                self.cpuDict[model] = {
                    "TDP": gConfig.getValue(f"{self.section}/CPUData/{model}/TDP", DEFAULT_TDP),
                    "Cores": gConfig.getValue(f"{self.section}/CPUData/{model}/Cores", 12),
                }

        self.log.info(f"Loaded {len(self.cpuDict)} CPU models")
        return S_OK()

    # =====================================================================
    #                           EXECUTE()
    # =====================================================================
    def execute(self):
        """Main agent loop: load jobs → compute metrics → send → store."""

        condDict = {"Status": [JobStatus.DONE, JobStatus.FAILED],
                    "ApplicationNumStatus": 0}

        result = self.jobDB.selectJobs(
            condDict, limit=self.maxJobsAtOnce,
            orderAttribute="LastUpdateTime:DESC",
        )
        if not result["OK"]:
            return result

        jobList = result["Value"]
        if not jobList:
            self.log.info("No jobs to process.")
            return S_OK()

        self.log.info(f"Loaded {len(jobList)} jobs")

        # Load job parameters
        params = self.elasticJobParametersDB.getJobParameters(jobList)
        attrs = self.jobDB.getJobsAttributes(jobList)

        if not params["OK"] or not attrs["OK"]:
            return S_ERROR("Failed to load job parameters")

        jobParamsDict, jobAttrDict = params["Value"], attrs["Value"]

        records = []
        for job in jobParamsDict:
            record = {}

            # Pick allowed fields
            for k, v in jobParamsDict[job].items():
                if k in JOB_PARAMETER_KEYS:
                    record[k] = v

            for k, v in jobAttrDict[job].items():
                if k in JOB_ATTRIBUTE_KEYS:
                    record[k] = str(v) if k in TIME_STAMPS else v

            records.append(record)

        # Mark processed
        self.jobDB.setJobAttributes(jobList, ["ApplicationNumStatus"], [9999])

        # -------------------------------------------------------------
        # Process each job record
        # -------------------------------------------------------------
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
            energy_kwh = self.__compute_energy_kwh(cpu_s, tdp, cores)
            energy_wh = energy_kwh * 1000.0
            emissions = energy_kwh * pue * ci  # gCO₂

            # =======================================================
            # INTERNAL RECORD (for JobDB)
            # =======================================================
            rec.update({
                "ExecUnitID": rec.get("JobID"),
                "Site": gocdb,
                "PUE": pue,
                "CI_g": ci,
                "Energy_wh": energy_wh,
                "CFP_g": emissions,
                "StartExecTime": rec.get("StartExecTime"),
                "StopExecTime": rec.get("EndExecTime"),
                "Status": rec.get("Status"),
                "Owner": Registry.getVOForGroup(rec.get("OwnerGroup")),
                "ExecUnitFinished": 1,

                # Work (Catalin definition)
                "Work": float(rec.get("NormCPUTime(s)", 0)) / (energy_wh or 1),

                # Grid detail
                "WallClockTime_s": float(rec.get("WallClockTime(s)", 0)),
                "CPUNormalizationFactor": rec.get("CPUNormalizationFactor"),
                "NCores": cores,
                "NormCPUTime_s": float(rec.get("NormCPUTime(s)", 0)),
                "Efficiency": float(rec.get("TotalCPUTime(s)", 0)) /
                              (float(rec.get("WallClockTime(s)", 1)) * cores),
                "TDP_w": tdp,
                "TotalCPUTime_s": float(rec.get("TotalCPUTime(s)", 0)),
                "ScaledCPUTime_s": float(rec.get("ScaledCPUTime(s)", 0)),
            })

            # =======================================================
            # CATALIN EUR RECORD (external)
            # =======================================================
            catalin_record = {
                "ExecUnitID": rec["ExecUnitID"],
                "Site": rec["Site"],
                "PUE": rec["PUE"],
                "CI_g": rec["CI_g"],
                "Energy_wh": rec["Energy_wh"],
                "CFP_g": rec["CFP_g"],
                "StartExecTime": rec["StartExecTime"],
                "StopExecTime": rec["StopExecTime"],
                "Status": rec["Status"],
                "Owner": rec["Owner"],
                "ExecUnitFinished": rec["ExecUnitFinished"],
                "Work": rec["Work"],

                # Grid detail fields:
                "WallClockTime_s": rec["WallClockTime_s"],
                "CPUNormalizationFactor": rec["CPUNormalizationFactor"],
                "NCores": rec["NCores"],
                "NormCPUTime_s": rec["NormCPUTime_s"],
                "Efficiency": rec["Efficiency"],
                "TDP_w": rec["TDP_w"],
                "TotalCPUTime_s": rec["TotalCPUTime_s"],
                "ScaledCPUTime_s": rec["ScaledCPUTime_s"],
            }

            self.log.info(
                f"🌱 JobID={rec.get('ExecUnitID')} "
                f"Energy={energy_kwh:.6f} kWh PUE={pue:.2f} CI={ci:.2f}"
            )

            # Send to CIM
            self.__sendRecordToMB(catalin_record)

            # Store internally
            self.__storeJobGreenMetrics(rec)

        return S_OK()

    # =====================================================================
    #                  API TOKENS & SITE PARAMETERS
    # =====================================================================

    def __get_jwt_token(self):
        """Authenticate with CIM API and cache JWT."""

        if self.token and self.token_ts:
            age = (time.time() - float(self.token_ts)) / 3600.0
            if age < self.token_max_age_hours:
                return self.token

        url = f"{self.cim_api_base.rstrip('/')}/get-token"
        r = requests.post(url, json={"email": self.login, "password": self.password}, timeout=10)
        r.raise_for_status()

        self.token = r.json().get("access_token")
        self.token_ts = time.time()

        self.log.info("🔐 Refreshed CIM JWT token")
        return self.token

    # -----------------------------------------------------
    def __getSiteParameters(self, site):
        """Retrieve PUE + CI from CIM service."""

        try:
            # Map DIRAC → GOCDB
            result = getDIRACGOCDictionary()
            gocdb_name = result["Value"].get(site, site) if result["OK"] else site

            headers = {
                "Authorization": f"Bearer {self.__get_jwt_token()}",
                "Content-Type": "application/json",
            }

            # ---- PUE request ----
            pue_url = f"{self.cim_api_base.rstrip('/')}/pue"
            pue_resp = requests.post(
                pue_url, headers=headers, json={"site_name": gocdb_name}, timeout=10
            )

            if pue_resp.status_code != 200:
                self.log.warn(f"PUE query failed for {gocdb_name}")
                return self.__getFallbackSiteParams(site, gocdb_name)

            pue_data = pue_resp.json()
            pue = float(pue_data.get("pue", DEFAULT_PUE))

            loc = pue_data.get("location", {})
            lat, lon = loc.get("latitude"), loc.get("longitude")
            if not lat or not lon:
                self.log.warn(f"No coordinates for {gocdb_name}")
                return self.__getFallbackSiteParams(site, gocdb_name, pue=pue)

            # ---- CI request (only offset_h = 5) ----
            offset_h = 5
            t = (datetime.now(timezone.utc) - timedelta(hours=offset_h))
            t = t.isoformat(timespec="seconds").replace("+00:00", "Z")

            ci_payload = {
                "lat": lat,
                "lon": lon,
                "pue": pue,
                "energy_wh": ENERGY_WH_DEFAULT,
                "time": t,
                "metric_id": gocdb_name,
                "wattnet_params": {"granularity": "hour"},
            }

            ci_url = f"{self.cim_api_base.rstrip('/')}/ci"
            ci_resp = requests.post(ci_url, headers=headers, json=ci_payload, timeout=10)

            if ci_resp.status_code == 200:
                ci_data = ci_resp.json()
                ci_raw = ci_data.get("ci_gco2_per_kwh")
                ci = ci_data.get("effective_ci_gco2_per_kwh") or (
                    ci_raw * pue if ci_raw else DEFAULT_CI
                )
            else:
                self.log.warn(f"CI query failed for {gocdb_name} → using DEFAULT_CI")
                ci = DEFAULT_CI

            return S_OK((pue, ci, gocdb_name))

        except Exception as e:
            self.log.error(f"Site parameter error for {site}: {e}")
            return self.__getFallbackSiteParams(site, site)

    # -----------------------------------------------------
    def __getFallbackSiteParams(self, site, gocdb_name, pue=None):
        grid = site.split(".")[0]
        pue = pue or gConfig.getValue(
            f"/Resources/Sites/{grid}/{site}/GreenParams/PUE", DEFAULT_PUE
        )
        ci = gConfig.getValue(
            f"/Resources/Sites/{grid}/{site}/GreenParams/CI", DEFAULT_CI
        )
        return S_OK((pue, ci, gocdb_name))

    # -----------------------------------------------------
    def __getProcessorParameters(self, model):
        if model in self.cpuDict:
            return S_OK((self.cpuDict[model]["TDP"], self.cpuDict[model]["Cores"]))

        self.log.warn(f"Unknown CPU model: {model}")
        return S_OK((DEFAULT_TDP, 12))

    # -----------------------------------------------------
    def __sendRecordToMB(self, record):
        """Send Catalin-compliant EUR to CIM API."""

        headers = {
            "Authorization": f"Bearer {self.__get_jwt_token()}",
            "Content-Type": "application/json",
        }

        resp = requests.post(
            self.metrics_db_url, headers=headers, json=record, timeout=15
        )

        self.log.info(f"Sent record to CIM API → {resp.status_code}")
        return S_OK()

    # =====================================================================
    #                  ENERGY MODEL + JOBDB STORAGE
    # =====================================================================
    def __compute_energy_kwh(self, cpu_seconds, tdp, cores):
        """Energy in kWh using CPU-time × (TDP/cores)."""
        try:
            cpu_seconds = float(cpu_seconds)
            if cpu_seconds < 0:
                return 0.0

            return cpu_seconds * tdp / cores / 3600.0 / 1000.0

        except Exception:
            return 0.0

    # -----------------------------------------------------
    def __storeJobGreenMetrics(self, record):
        """Store green metrics inside DIRAC JobDB."""

        jobID = record.get("ExecUnitID")
        if not jobID:
            self.log.error("Cannot store metrics: missing JobID")
            return

        params = [
            ("Energy_wh", record.get("Energy_wh")),
            ("CI_g", record.get("CI_g")),
            ("CFP_g", record.get("CFP_g")),
            ("PUE", record.get("PUE")),
            ("ExecUnitFinished", record.get("ExecUnitFinished")),
            ("Work", record.get("Work")),
            ("TDP_w", record.get("TDP_w")),
            ("TotalCPUTime_s", record.get("TotalCPUTime_s")),
            ("WallClockTime_s", record.get("WallClockTime_s")),
            ("NCores", record.get("NCores")),
        ]

        result = self.jobDB.setJobParameters(jobID, params)

        if not result["OK"]:
            self.log.error(f"❌ Failed JobDB write for {jobID}: {result['Message']}")
        else:
            self.log.info(f"💾 Stored green metrics in JobDB for JobID={jobID}")
