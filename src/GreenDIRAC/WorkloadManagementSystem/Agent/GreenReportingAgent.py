#!/usr/bin/env python3
"""
GreenReportingAgent — Queries CIMClient for site green metrics,
submits per-job green metrics to CIM,
and stores them in DIRAC JobDB / ElasticSearch.
"""

from datetime import timezone

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client import PathFinder

from GreenDIRAC.WorkloadManagementSystem.Client.CIMClient import CIMClient


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
    "JobGroup", "JobName", "Owner", "OwnerDN", "OwnerGroup",
    "RescheduleCounter", "Site",
    "SubmissionTime", "StartExecTime", "EndExecTime",
    "SystemPriority", "UserPriority",
]

TIME_STAMPS = ["SubmissionTime", "StartExecTime", "EndExecTime"]

DEFAULT_TDP = 150


# ==========================================================
#               GreenReportingAgent
# ==========================================================
class GreenReportingAgent(AgentModule):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.jobDB = None
        self.elasticJobParametersDB = None
        self.maxJobsAtOnce = 50

        self.section = PathFinder.getAgentSection(self.agentName)

        # CIM abstraction
        self.cimClient = None

    # -----------------------------------------------------
    def initialize(self):

        self.jobDB = JobDB()

        # ElasticSearch support
        self.elasticJobParametersDB = None
        useES = Operations().getValue(
            "/Services/JobMonitoring/useESForJobParametersFlag", False
        )

        if useES:
            res = ObjectLoader().loadObject(
                "WorkloadManagementSystem.DB.ElasticJobParametersDB",
                "ElasticJobParametersDB",
            )
            if res["OK"]:
                self.elasticJobParametersDB = res["Value"](parentLogger=self.log)
                self.log.info("Using ElasticJobParametersDB")
            else:
                self.log.warn("Falling back to JobDB for job parameters")

        self.maxJobsAtOnce = self.am_getOption(
            "MaxJobsAtOnce", self.maxJobsAtOnce
        )

        # Instantiate CIM client
        self.cimClient = CIMClient()

        # Load CPU models
        self.cpuDict = {}
        res = gConfig.getSections(f"{self.section}/CPUData")
        if res["OK"]:
            for model in res["Value"]:
                self.cpuDict[model] = {
                    "TDP": gConfig.getValue(
                        f"{self.section}/CPUData/{model}/TDP", DEFAULT_TDP
                    ),
                    "Cores": gConfig.getValue(
                        f"{self.section}/CPUData/{model}/Cores", 12
                    ),
                }

        self.log.info(f"Loaded {len(self.cpuDict)} CPU models")
        return S_OK()

    # =====================================================================
    # EXECUTE
    # =====================================================================
    def execute(self):

        condDict = {
            "Status": [JobStatus.DONE, JobStatus.FAILED],
            "ApplicationNumStatus": 0,
        }

        res = self.jobDB.selectJobs(
            condDict,
            limit=self.maxJobsAtOnce,
            orderAttribute="LastUpdateTime:DESC",
        )
        if not res["OK"]:
            return res

        jobIDs = [int(j) for j in res["Value"]]
        if not jobIDs:
            return S_OK()

        # Load job parameters
        if self.elasticJobParametersDB:
            params = self.elasticJobParametersDB.getJobParameters(jobIDs)
        else:
            params = self.jobDB.getJobParameters(jobIDs)

        attrs = self.jobDB.getJobsAttributes(jobIDs)

        if not params["OK"] or not attrs["OK"]:
            return S_ERROR("Failed to load job data")

        jobParamsDict = params["Value"]
        jobAttrDict = attrs["Value"]

        records = []

        for jobID in jobParamsDict:
            rec = {}

            for k, v in jobParamsDict[jobID].items():
                if k in JOB_PARAMETER_KEYS:
                    rec[k] = v

            for k, v in jobAttrDict.get(jobID, {}).items():
                if k in JOB_ATTRIBUTE_KEYS:
                    rec[k] = str(v) if k in TIME_STAMPS else v

            rec["JobID"] = int(jobID)
            records.append(rec)

        # Mark processed
        self.jobDB.setJobAttributes(
            jobIDs, ["ApplicationNumStatus"], [9999]
        )

        # -------------------------------------------------------------
        # Process jobs
        # -------------------------------------------------------------
        for rec in records:

            tdp, cores = self.__getProcessorParameters(
                rec.get("ModelName", "Unknown")
            )

            site = rec.get("Site", "Unknown")

            # ---- READ from CIM ----
            pue, ci, gocdb = self.cimClient.getSiteGreenMetrics(site)

            rec["SiteDIRAC"] = site
            rec["SiteGOCDB"] = gocdb
            rec["Site"] = gocdb

            cpu_s = float(rec.get("TotalCPUTime(s)", 0))
            energy_kwh = self.__compute_energy_kwh(cpu_s, tdp, cores)
            energy_wh = energy_kwh * 1000.0
            emissions = energy_kwh * pue * ci

            cpunorm = float(rec.get("CPUNormalizationFactor", 0))
            cee = (cpunorm * cores) / float(tdp) if tdp else 0.0

            rec.update({
                "ExecUnitID": rec["JobID"],
                "PUE": pue,
                "CI_g": ci,
                "Energy_wh": energy_wh,
                "CFP_g": emissions,
                "Owner": Registry.getVOForGroup(rec.get("OwnerGroup")),
                "ExecUnitFinished": 1,
                "NCores": cores,
                "TDP_w": tdp,
                "CEE": cee,
            })

            # -------------------------------------------------
            # SUBMIT to CIM
            # -------------------------------------------------
            try:
                ok = self.cimClient.submitRecord(rec)
                if ok:
                    self.log.info(
                        f"CIM submission OK for JobID={rec['ExecUnitID']} "
                        f"Site={gocdb}"
                    )
                else:
                    self.log.error(
                        f"CIM submission FAILED for JobID={rec['ExecUnitID']}"
                    )
            except Exception as e:
                self.log.exception(
                    f"CIM submission EXCEPTION for JobID={rec['ExecUnitID']}: {e}"
                )

            # -------------------------------------------------
            # STORE in ElasticSearch
            # -------------------------------------------------
            if self.__storeJobGreenMetrics(rec):
                self.log.info(
                    f"ElasticSearch storage OK for JobID={rec['ExecUnitID']}"
                )

        return S_OK()

    # =====================================================================
    # HELPERS
    # =====================================================================
    def __getProcessorParameters(self, model):
        if model in self.cpuDict:
            cpu = self.cpuDict[model]
            return cpu["TDP"], cpu["Cores"]
        self.log.warn(f"Unknown CPU model: {model}")
        return DEFAULT_TDP, 12

    def __compute_energy_kwh(self, cpu_seconds, tdp, cores):
        try:
            return float(cpu_seconds) * tdp / cores / 3600.0 / 1000.0
        except Exception:
            return 0.0

    def __storeJobGreenMetrics(self, record):
        jobID = record.get("ExecUnitID")
        if not jobID or not self.elasticJobParametersDB:
            return False

        es_params = {
            k: (str(v) if k in TIME_STAMPS else v)
            for k, v in record.items()
            if v is not None
        }

        res = self.elasticJobParametersDB.setJobParameters(jobID, es_params)
        if not res["OK"]:
            self.log.error(
                f"ElasticSearch write failed for JobID={jobID}: "
                f"{res.get('Message')}"
            )
            return False

        return True
