#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AverageCEEAgent
---------------
DIRAC Agent that computes an online (Welford) Average CEE per GridCE
based on recent jobs stored in ElasticSearch (ES), and writes the result
into the DIRAC Configuration System.

Stored values:
    /Resources/Sites/<Grid>/<Site>/CEs/<CEName>/AverageCEE
    /Resources/Sites/<Grid>/<Site>/CEs/<CEName>/AverageCEECount
"""

from collections import defaultdict
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC import S_OK, S_ERROR


# ======================================================================
#  AGENT ## This agent perhaps better to be changed to read directly from ES
#  once we have to ES
# ======================================================================

class AverageCEEAgent(AgentModule):

    def initialize(self):

        # NEW: get agent section for storing LastProcessedJobID
        self.section = PathFinder.getAgentSection(self.agentName)

        self.limit = int(self.am_getOption("Limit", 200))

        # Check ES availability
        useES = Operations().getValue(
            "/Services/JobMonitoring/useESForJobParametersFlag", False
        )
        if not useES:
            self.log.error("ElasticSearch is disabled in DIRAC configuration")
            return S_ERROR("ES disabled")

        # Load ES DB
        result = ObjectLoader().loadObject(
            "WorkloadManagementSystem.DB.ElasticJobParametersDB",
            "ElasticJobParametersDB",
        )
        if not result["OK"]:
            msg = result["Message"]
            self.log.error("Cannot load ElasticJobParametersDB:", msg)
            return S_ERROR(msg)

        self.esDB = result["Value"]()
        self.log.info("AverageCEEAgent initialization complete")
        return S_OK()


    # ===================================================================
    def execute(self):

        self.log.info("🟢 AverageCEEAgent cycle started")

        # ------------------------------------------------------
        # Load last processed JobID from agent CS section (NEW)
        # ------------------------------------------------------
        lastProcessed = gConfig.getValue(f"{self.section}/LastProcessedJobID", 0)
        try:
            lastProcessed = int(lastProcessed)
        except Exception:
            lastProcessed = 0

        # ------------------------------------------------------
        # Step 1 — Load recent processed jobs from JobDB
        # ------------------------------------------------------
        allJobIDs = self._getProcessedJobIDs(self.limit)

        # Keep only NEW jobs (NEW)
        jobIDs = [jid for jid in allJobIDs if jid > lastProcessed]

        self.log.info(f"Filtered {len(jobIDs)} new jobs out of {len(allJobIDs)} total")

        if not jobIDs:
            self.log.info("No new jobs to process.")
            return S_OK()

        # ------------------------------------------------------
        # Step 2 — Build mapping GridCE → (Grid, Site, CEName)
        # ------------------------------------------------------
        ceLookup = self._build_CE_lookup()

        # ------------------------------------------------------
        # Step 3 — Collect per-job CEE values per GridCE
        # ------------------------------------------------------
        ceeLists = self._collectCEEvalues(jobIDs)

        # ------------------------------------------------------
        # Step 4 — Update AverageCEE using Welford online method
        # ------------------------------------------------------
        self._updateAverageCEE(ceeLists, ceLookup)

        # ------------------------------------------------------
        # Step 5 — Save new LastProcessedJobID into agent CS section (NEW)
        # ------------------------------------------------------
        newLast = max(jobIDs)
        csAPI = CSAPI()
        csAPI.setOption(f"{self.section}/LastProcessedJobID", str(newLast))
        commit = csAPI.commit()

        if commit["OK"]:
            self.log.info(f"Updated LastProcessedJobID to {newLast}")
        else:
            self.log.error(f"Failed to update LastProcessedJobID: {commit['Message']}")

        self.log.info("🔚 AverageCEEAgent cycle completed")
        return S_OK()


    # ===================================================================
    #           LOAD JOB IDS
    # ===================================================================
    def _getProcessedJobIDs(self, limit):

        jobDB = JobDB()
        condDict = {"ApplicationNumStatus": 9999}

        self.log.info(f"Fetching last {limit} processed jobs…")

        res = jobDB.selectJobs(
            condDict,
            limit=limit,
            orderAttribute="JobID:DESC",
        )
        if not res["OK"]:
            self.log.error("JobDB error:", res["Message"])
            return []

        jobIDs = [int(jid) for jid in res["Value"]]
        self.log.info(f"Retrieved {len(jobIDs)} processed jobs")
        return jobIDs


    # ===================================================================
    #           BUILD GRIDCE → (Grid, Site, CEName)
    # ===================================================================
    def _build_CE_lookup(self):

        ceMap = {}

        resGrid = gConfig.getSections("/Resources/Sites")
        if not resGrid["OK"]:
            self.log.error("Cannot read /Resources/Sites")
            return ceMap

        raw_grids = resGrid["Value"]
        grids = [g for g in raw_grids if g not in ("OK", "Value", "", None)]

        for grid in grids:
            resSite = gConfig.getSections(f"/Resources/Sites/{grid}")
            if not resSite["OK"]:
                continue

            for site in resSite["Value"]:

                # list of hostnames under CE=
                ceList_raw = gConfig.getValue(
                    f"/Resources/Sites/{grid}/{site}/CE", ""
                )
                if not ceList_raw:
                    continue

                ceHostnames = [c.strip() for c in ceList_raw.split(",") if c.strip()]

                # CE blocks under /CEs
                ceBase = f"/Resources/Sites/{grid}/{site}/CEs"
                resCEs = gConfig.getSections(ceBase)
                if not resCEs["OK"]:
                    continue

                for ceName in resCEs["Value"]:
                    if ceName in ceHostnames:
                        ceMap[ceName] = (grid, site, ceName)

        self.log.info(f"CE mapping loaded: {len(ceMap)} entries")
        return ceMap


    # ===================================================================
    #           COLLECT ALL CEE VALUES PER GRIDCE
    # ===================================================================
    def _collectCEEvalues(self, jobIDs):

        ceeValues = defaultdict(list)

        for jobID in jobIDs:

            try:
                res = self.esDB.getJobParameters(jobID)
            except Exception as e:
                self.log.warn(f"ES error for job {jobID}: {e}")
                continue

            if not res["OK"]:
                continue

            params = res["Value"] or {}
            job = params.get(jobID, params)

            gridCE = job.get("GridCE")
            cee = job.get("CEE")

            if not gridCE or cee is None:
                continue

            try:
                ceeValues[gridCE].append(float(cee))
            except Exception:
                continue

        self.log.info(f"Collected CEE lists for {len(ceeValues)} GridCEs")
        return ceeValues


    # ===================================================================
    #           WELFORD ONLINE UPDATE
    # ===================================================================
    def _updateAverageCEE(self, ceeLists, ceLookup):

        csAPI = CSAPI()
        csDirty = False

        for gridCE, cee_list in ceeLists.items():

            if gridCE not in ceLookup:
                self.log.warn(f"No CS mapping for GridCE {gridCE}")
                continue

            grid, site, ceName = ceLookup[gridCE]
            cePath = f"/Resources/Sites/{grid}/{site}/CEs/{ceName}"

            # Load previous online stats
            old_avg = gConfig.getValue(f"{cePath}/AverageCEE", None)
            old_count = gConfig.getValue(f"{cePath}/AverageCEECount", 0)

            try:
                old_avg = float(old_avg) if old_avg is not None else None
                old_count = int(old_count)
            except Exception:
                self.log.error(f"Corrupted AverageCEE state for {ceName}, resetting.")
                old_avg = None
                old_count = 0

            # Initial state
            mean = old_avg
            count = old_count

            # ----------------------------------------------------
            # Apply Welford update per job
            # ----------------------------------------------------
            for x in cee_list:
                x = float(x)
                if mean is None:
                    mean = x
                    count = 1
                else:
                    count += 1
                    delta = x - mean
                    mean = mean + delta / count

            if mean is None:
                continue

            new_avg_str = f"{mean:.6f}"

            self.log.info(
                f"Updating {cePath}/AverageCEE = {new_avg_str} (samples={count})"
            )

            csAPI.setOption(f"{cePath}/AverageCEE", new_avg_str)
            csAPI.setOption(f"{cePath}/AverageCEECount", str(count))
            csDirty = True

        # --------------------------------------------------------
        # Commit
        # --------------------------------------------------------
        if csDirty:
            commit = csAPI.commit()
            if commit["OK"]:
                self.log.info("CS commit successful")
            else:
                self.log.error("CS commit FAILED:", commit["Message"])
        else:
            self.log.info("No CS changes to commit")
