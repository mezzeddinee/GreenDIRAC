# -*- coding: utf-8 -*-
"""
GreenDIRAC SiteDirector with GD-CI integration.

- Authenticates to CIM API (JWT cached).
- Queries GD-CI /pue to get PUE + site lat/lon.
- Queries GD-CI /ci with lat/lon + PUE to compute effective CI.
- Caches (PUE, CI) per site for 3 minutes to avoid hammering endpoints.
- Sorts queues by "green efficiency" (30% PUE, 70% CI) and logs Top 5.
- Falls back gracefully (PUE=5.0, CI=9999.0) on errors.
"""

from __future__ import annotations

import os
import random
import socket
import sys
import time
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import DIRAC
from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.Types.PilotSubmission import PilotSubmission as PilotSubmissionAccounting
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.TimeUtilities import second, toEpochMilliSeconds
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.PilotScopes import PILOT_SCOPES

from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getPilotAgentsDB
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import getGridEnv
from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import (
    _writePilotWrapperFile,
    getPilotFilesCompressedEncodedDict,
    pilotWrapperScript,
)

from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved


MAX_PILOTS_TO_SUBMIT = 100


class SiteDirector(AgentModule):
    """SiteDirector class provides an implementation of a DIRAC agent.

    Used for submitting pilots to Computing Elements.
    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        # on-the fly imports
        ol = ObjectLoader()
        res = ol.loadModule("ConfigurationSystem.Client.Helpers.Resources")
        if not res["OK"]:
            sys.exit(res["Message"])
        self.resourcesModule = res["Value"]

        self.queueDict = {}
        # self.queueCECache aims at saving CEs information over the cycles to avoid to create the exact same CEs each cycle
        self.queueCECache = {}
        self.queueSlots = {}
        self.failedQueues = defaultdict(int)
        # failedPilotOutput stores the number of times the Site Director failed to get a given pilot output
        self.failedPilotOutput = defaultdict(int)
        self.firstPass = True
        self.maxPilotsToSubmit = MAX_PILOTS_TO_SUBMIT

        self.gridEnv = ""
        self.vo = ""
        self.group = ""
        # self.voGroups contain all the eligible user groups for pilots submitted by this SiteDirector
        self.voGroups = []
        self.pilotDN = ""
        self.pilotGroup = ""
        self.platforms = []
        self.sites = []
        self.totalSubmittedPilots = 0

        self.addPilotsToEmptySites = False
        self.checkPlatform = False
        self.updateStatus = True
        self.getOutput = False
        self.sendAccounting = True
        self.sendSubmissionAccounting = True
        self.sendSubmissionMonitoring = False
        self.siteClient = None
        self.rssClient = None
        self.pilotAgentsDB = None
        self.rssFlag = None

        self.globalParameters = {"NumberOfProcessors": 1, "MaxRAM": 2048}
        # self.failedQueueCycleFactor is the number of cycles a queue has to wait before getting pilots again
        self.failedQueueCycleFactor = 10
        # Every N cycles, the status of the pilots are updated by the SiteDirector
        self.pilotStatusUpdateCycleFactor = 10
        # Every N cycles, the number of slots available in the queues is updated
        self.availableSlotsUpdateCycleFactor = 10
        self.maxQueueLength = 86400 * 3
        # Maximum number of times the Site Director is going to try to get a pilot output before stopping
        self.maxRetryGetPilotOutput = 3
        # Bundle proxy lifetime factor
        self.bundleProxyLifetimeFactor = 1.5

        self.pilotWaitingFlag = True
        self.pilotLogLevel = "INFO"
        self.matcherClient = None
        self.siteMaskList = []
        self.ceMaskList = []

        self.localhost = socket.getfqdn()

        ###########################################################################
        # 🌿 Green Computing Metrics (GD-CI / CIM integration)
        ###########################################################################
        # API base URLs
        self.cim_api_base = self.am_getOption("CIM_API_BASE", "https://mc-a4.lab.uvalight.net/gd-cim-api")
        self.gd_ci_api_base = self.am_getOption("GD_CI_API_BASE", "https://mc-a4.lab.uvalight.net/gd-ci-api")

        # ⚠️ Credentials (replace with secure storage in production)
        self.cim_email = self.am_getOption("CIM_EMAIL", "atsareg@in2p3.fr")
        self.cim_password = self.am_getOption("CIM_PASSWORD", "Green@31415")

        # Token cache
        self.token = None
        self.token_ts = None
        self.token_max_age_hours = float(self.am_getOption("TOKEN_MAX_AGE_HOURS", 24))

        # PUE/CI cache (3 minutes)
        self.green_cache = {}
        self.green_cache_ttl = 180  # seconds

    # --------------------------------------------------------------------------
    # Standard Agent lifecycle
    # --------------------------------------------------------------------------
    def initialize(self):
        """Initial settings"""

        self.gridEnv = self.am_getOption("GridEnv", "")
        if not self.gridEnv:
            self.gridEnv = getGridEnv()

        # The SiteDirector is for a particular user community
        self.vo = self.am_getOption("VO", "")
        if not self.vo:
            self.vo = self.am_getOption("Community", "")
        if not self.vo:
            self.vo = CSGlobals.getVO()
        # The SiteDirector is for a particular user group
        self.group = self.am_getOption("Group", "")

        # Choose the group for which pilots will be submitted. This is a hack until
        # we will be able to match pilots to VOs.
        if not self.group:
            if self.vo:
                result = Registry.getGroupsForVO(self.vo)
                if not result["OK"]:
                    return result
                self.voGroups = []
                for group in result["Value"]:
                    if "NormalUser" in Registry.getPropertiesForGroup(group):
                        self.voGroups.append(group)
        else:
            self.voGroups = [self.group]

        # Get the clients
        self.siteClient = SiteStatus()
        self.rssClient = ResourceStatus()
        self.matcherClient = MatcherClient()
        self.pilotAgentsDB = getPilotAgentsDB()

        return S_OK()

    def beginExecution(self):
        """Run at every cycle, as first thing."""
        self.rssFlag = self.rssClient.rssFlag

        # Which credentials to use?
        self.pilotDN = self.am_getOption("PilotDN", self.pilotDN)
        self.pilotGroup = self.am_getOption("PilotGroup", self.pilotGroup)
        result = findGenericPilotCredentials(vo=self.vo, pilotDN=self.pilotDN, pilotGroup=self.pilotGroup)
        if not result["OK"]:
            return result
        self.pilotDN, self.pilotGroup = result["Value"]

        # Parameters
        self.workingDirectory = self.am_getOption("WorkDirectory")
        self.maxQueueLength = self.am_getOption("MaxQueueLength", self.maxQueueLength)
        self.pilotLogLevel = self.am_getOption("PilotLogLevel", self.pilotLogLevel)
        self.maxPilotsToSubmit = self.am_getOption("MaxPilotsToSubmit", self.maxPilotsToSubmit)
        self.pilotWaitingFlag = self.am_getOption("PilotWaitingFlag", self.pilotWaitingFlag)
        self.failedQueueCycleFactor = self.am_getOption("FailedQueueCycleFactor", self.failedQueueCycleFactor)
        self.pilotStatusUpdateCycleFactor = self.am_getOption(
            "PilotStatusUpdateCycleFactor", self.pilotStatusUpdateCycleFactor
        )
        self.availableSlotsUpdateCycleFactor = self.am_getOption(
            "AvailableSlotsUpdateCycleFactor", self.availableSlotsUpdateCycleFactor
        )
        self.maxRetryGetPilotOutput = self.am_getOption("MaxRetryGetPilotOutput", self.maxRetryGetPilotOutput)

        # Flags
        self.addPilotsToEmptySites = self.am_getOption("AddPilotsToEmptySites", self.addPilotsToEmptySites)
        self.checkPlatform = self.am_getOption("CheckPlatform", self.checkPlatform)
        self.updateStatus = self.am_getOption("UpdatePilotStatus", self.updateStatus)
        self.getOutput = self.am_getOption("GetPilotOutput", self.getOutput)
        self.sendAccounting = self.am_getOption("SendPilotAccounting", self.sendAccounting)

        # Check whether to send to Monitoring or Accounting or both
        monitoringOption = Operations().getMonitoringBackends(monitoringType="PilotSubmissionMonitoring")
        if "Monitoring" in monitoringOption:
            self.sendSubmissionMonitoring = True
        if "Accounting" in monitoringOption:
            self.sendSubmissionAccounting = True

        # Get the site description dictionary
        siteNames = None
        siteNamesOption = self.am_getOption("Site", ["any"])
        if siteNamesOption and "any" not in [sn.lower() for sn in siteNamesOption]:
            siteNames = siteNamesOption

        ceTypes = None
        ceTypesOption = self.am_getOption("CETypes", ["any"])
        if ceTypesOption and "any" not in [ct.lower() for ct in ceTypesOption]:
            ceTypes = ceTypesOption

        ces = None
        cesOption = self.am_getOption("CEs", ["any"])
        if cesOption and "any" not in [ce.lower() for ce in cesOption]:
            ces = cesOption

        tags = self.am_getOption("Tags", [])
        if not tags:
            tags = None

        self.log.always("VO:", self.vo)
        if self.voGroups:
            self.log.always("Group(s):", self.voGroups)
        self.log.always("Sites:", siteNames)
        self.log.always("CETypes:", ceTypes)
        self.log.always("CEs:", ces)
        self.log.always("PilotDN:", self.pilotDN)
        self.log.always("PilotGroup:", self.pilotGroup)

        result = self.resourcesModule.getQueues(
            community=self.vo, siteList=siteNames, ceList=ces, ceTypeList=ceTypes, tags=tags
        )
        if not result["OK"]:
            return result
        result = getQueuesResolved(
            siteDict=result["Value"],
            queueCECache=self.queueCECache,
            gridEnv=self.gridEnv,
            setup=gConfig.getValue("/DIRAC/Setup", "unknown"),
            workingDir=self.workingDirectory,
            checkPlatform=self.checkPlatform,
            instantiateCEs=True,
        )
        if not result["OK"]:
            return result

        self.queueDict = result["Value"]
        self.platforms = []
        self.sites = []
        for __queueName, queueDict in self.queueDict.items():
            # Update self.sites
            if queueDict["Site"] not in self.sites:
                self.sites.append(queueDict["Site"])

            # Update self.platforms, keeping entries unique and squashing lists
            if "Platform" in queueDict["ParametersDict"]:
                platform = queueDict["ParametersDict"]["Platform"]
                oldPlatforms = set(self.platforms)
                if isinstance(platform, list):
                    oldPlatforms.update(set(platform))
                else:
                    oldPlatforms.add(platform)
                self.platforms = list(oldPlatforms)

            # Update self.globalParameters
            if "WholeNode" in queueDict["ParametersDict"]:
                self.globalParameters["WholeNode"] = "True"
            for parameter in ["MaxRAM", "NumberOfProcessors"]:
                if parameter in queueDict["ParametersDict"]:
                    self.globalParameters[parameter] = max(
                        self.globalParameters[parameter], int(queueDict["ParametersDict"][parameter])
                    )

        if self.updateStatus:
            self.log.always("Pilot status update requested")
        if self.getOutput:
            self.log.always("Pilot output retrieval requested")
        if self.sendAccounting:
            self.log.always("Pilot accounting sending requested")
        if self.sendSubmissionAccounting:
            self.log.always("Pilot submission accounting sending requested")
        if self.sendSubmissionMonitoring:
            self.log.always("Pilot submission monitoring sending requested")

        self.log.always("MaxPilotsToSubmit:", self.maxPilotsToSubmit)

        if self.firstPass:
            if self.queueDict:
                self.log.always("Agent will serve queues:")
                for queue in self.queueDict:
                    self.log.always(
                        f"Site: {self.queueDict[queue]['Site']}, CE: {self.queueDict[queue]['CEName']}, Queue: {queue}"
                    )
        self.firstPass = False

        return S_OK()

    def execute(self):
        """Main execution method (called at each agent cycle)."""
        if not self.queueDict:
            self.log.warn("No site defined, exiting the cycle")
            return S_OK()

        # get list of usable sites within this cycle
        result = self.siteClient.getUsableSites()
        if not result["OK"]:
            return result
        self.siteMaskList = result.get("Value", [])

        if self.rssFlag:
            ceNamesList = [queue["CEName"] for queue in self.queueDict.values()]
            result = self.rssClient.getElementStatus(ceNamesList, "ComputingElement", vO=self.vo)
            if not result["OK"]:
                self.log.error("Can not get the status of computing elements: ", result["Message"])
                return result
            # Try to get CEs which have been probed and those unprobed (vO='all').
            self.ceMaskList = [
                ceName for ceName in result["Value"] if result["Value"][ceName]["all"] in ("Active", "Degraded")
            ]
            self.log.debug("CE list with status Active or Degraded: ", self.ceMaskList)

        result = self.submitPilots()
        if not result["OK"]:
            self.log.error("Errors in the job submission: ", result["Message"])
            return result

        # Every N cycles we update the pilots status
        cyclesDone = self.am_getModuleParam("cyclesDone")
        if self.updateStatus and cyclesDone % self.pilotStatusUpdateCycleFactor == 0:
            result = self.updatePilotStatus()
            if not result["OK"]:
                self.log.error("Errors in updating pilot status: ", result["Message"])
                return result

        return S_OK()

    # --------------------------------------------------------------------------
    # 🌿 GD-CI / CIM integration helpers
    # --------------------------------------------------------------------------
    def __get_cim_jwt_token(self):
        """Get and cache a JWT token from the CIM API."""
        # Reuse valid cached token if still fresh
        if self.token and self.token_ts:
            age_hours = (time.time() - self.token_ts) / 3600.0
            if age_hours < self.token_max_age_hours:
                return self.token

        url = f"{self.cim_api_base.rstrip('/')}/get-token"
        payload = {
            "email": self.cim_email,
            "password": self.cim_password,
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code != 200:
                self.log.warn(f"⚠️ CIM login failed: HTTP {response.status_code}")
                # Return None; downstream will try unauthenticated requests
                return None

            data = response.json()
            token = data.get("access_token")
            if not token:
                self.log.warn("⚠️ No token found in CIM login response")
                return None

            # Cache token with timestamp
            self.token = token
            self.token_ts = time.time()
            self.log.info("🔐 New CIM JWT token acquired and cached")
            return token

        except requests.RequestException as e:
            self.log.warn(f"🚨 Error while getting CIM JWT token: {e}")
            return None

    def __resolve_dirac_site_name_for_query(self, site_name: str) -> str:
        """Normalize a DIRAC site name to the expected GD-CI 'site_name' format."""
        if not site_name:
            return ""

        # Typical DIRAC formats: "EGI.IN2P3-CC.fr" → "IN2P3-CC"
        site_query = site_name

        # Remove EGI./LCG. prefix
        if site_query.startswith("EGI.") or site_query.startswith("LCG."):
            site_query = site_query.split(".", 1)[1]

        # Remove trailing country code if short (.fr, .it, .uk, .ch, ...)
        parts = site_query.split(".")
        if len(parts) > 1 and 2 <= len(parts[-1]) <= 3:
            site_query = ".".join(parts[:-1])

        site_query = site_query.strip()
        self.log.verbose(f"Resolved '{site_name}' → '{site_query}' for GD-CI query")
        return site_query

    def __get_green_metrics_for_site(self, site: str):
        """Retrieve (PUE, CI) for a site from the GD-CI API, cached for 3 minutes."""
        if not site:
            return S_ERROR("Missing site name")

        # Cache check (3-minute TTL)
        if site in self.green_cache:
            ts, pue, ci = self.green_cache[site]
            if time.time() - ts < self.green_cache_ttl:
                return S_OK((pue, ci))

        token = self.__get_cim_jwt_token()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            self.log.warn("Proceeding without CIM token (unauthenticated requests)")

        # Prefer the configured Name for the site if available
        site_query = self.__resolve_dirac_site_name_for_query(site)
        self.log.verbose(f"Querying GD-CI API for site_name='{site_query}' (from {site})")

        url_pue = f"{self.gd_ci_api_base.rstrip('/')}/pue"
        try:
            # Step 1️⃣ — Get PUE and location
            r = requests.post(url_pue, headers=headers, json={"site_name": site_query}, timeout=10)
            self.log.verbose(f"GD-CI PUE {site_query}: HTTP {r.status_code}")

            # If not found/unauthorized, fallback
            if r.status_code in (401, 404):
                pue, ci = 5.0, 9999.0
                self.green_cache[site] = (time.time(), pue, ci)
                return S_OK((pue, ci))

            r.raise_for_status()
            data = r.json()
            pue = float(data.get("pue", 5.0))

            # Extract coordinates for CI query
            loc = data.get("location", {}) or {}
            lat = loc.get("latitude")
            lon = loc.get("longitude")

            if lat is None or lon is None:
                pue, ci = pue, 9999.0
                self.green_cache[site] = (time.time(), pue, ci)
                self.log.verbose(f"No coordinates for {site_query} — fallback to defaults.")
                return S_OK((pue, ci))

            # --- Step 2️⃣: Query /ci endpoint dynamically using lat/lon ---
            url_ci = f"{self.gd_ci_api_base.rstrip('/')}/ci"
            payload_ci = {
                "lat": lat,
                "lon": lon,
                "pue": pue,
                "energy_wh": 8500,
                "time": datetime.utcnow().isoformat() + "Z",
                "metric_id": f"{site_query}-gdci",
                "wattnet_params": {"granularity": "hour"},
            }

            rc = requests.post(url_ci, headers=headers, json=payload_ci, timeout=15)
            self.log.verbose(f"GD-CI CI query for {site_query}: HTTP {rc.status_code}")

            if rc.status_code == 200:
                ci_data = rc.json()
                # Try multiple common keys
                ci_val = (
                    ci_data.get("effective_ci_gco2_per_kwh")
                    or ci_data.get("ci_gco2_per_kwh")
                    or ci_data.get("carbon_intensity")
                )
                ci = float(ci_val) if ci_val is not None else 9999.0
            else:
                ci = 9999.0

            # Cache & return
            self.green_cache[site] = (time.time(), pue, ci)
            self.log.info(f"🌱 {site}: PUE={pue:.2f}, CI={ci:.2f} gCO₂/kWh")
            return S_OK((pue, ci))

        except requests.RequestException as e:
            self.log.warn(f"GD-CI API error for {site_query}: {e}")
            pue, ci = 5.0, 9999.0
            self.green_cache[site] = (time.time(), pue, ci)
            return S_OK((pue, ci))

    # --------------------------------------------------------------------------
    # 🧩 submitPilots() with live GD-CI integration and green sorting
    # --------------------------------------------------------------------------
    def submitPilots(self):
        """Go through defined computing elements and submit pilots if necessary and possible"""

        # First, we check if we want to submit pilots at all, and also where
        submit, anySite, jobSites, testSites = self._ifAndWhereToSubmit()
        if not submit:
            self.log.notice("Not submitting any pilots at this cycle")
            return S_OK()

        self.log.debug("Going to try to submit some pilots")
        self.log.verbose("Queues treated", ",".join(self.queueDict))
        self.totalSubmittedPilots = 0

        # ---------------------------------------------------------------------
        # 🔋 Enrich queueDict with live PUE and CI values from GD-CI API
        # ---------------------------------------------------------------------
        for queueName, queueData in self.queueDict.items():
            site = queueData.get("Site", "")
            params = queueData["ParametersDict"]

            result = self.__get_green_metrics_for_site(site)
            if result["OK"]:
                pue, ci = result["Value"]
                params["PUE"] = round(float(pue), 2)
                params["CI"] = round(float(ci), 2)
                self.log.info(f"🌱 {site}: PUE={params['PUE']:.2f}, CI={params['CI']:.2f} gCO₂/kWh")
            else:
                # Absolute fallback (should not be reached because helper already degrades gracefully)
                params["PUE"] = 5.0
                params["CI"] = 9999.0

        # ---------------------------------------------------------------------
        # ⚙️ Combined efficiency function for sorting queues
        # ---------------------------------------------------------------------
        def combined_efficiency(item, w_pue=0.3, w_ci=0.7):
            """Weighted score (lower is greener)."""
            _queue_name, queue_data = item
            params = queue_data.get("ParametersDict", {})
            pue = float(params.get("PUE", 1.5))
            ci = float(params.get("CI", 400.0))

            # Normalize to ~0..1 ranges
            pue_norm = max(0.0, (pue - 1.0) / 1.0)  # ~1.0–2.0 → 0–1, clamp at 0
            ci_norm = max(0.0, min(ci / 800.0, 1.0))  # assume 0–800 for normalization

            score = w_pue * pue_norm + w_ci * ci_norm
            return score

        # ---------------------------------------------------------------------
        # 🔢 Sort queues by green efficiency (favoring low CI)
        # ---------------------------------------------------------------------
        queueDictItems = list(self.queueDict.items())
        queueDictItems.sort(key=lambda item: combined_efficiency(item))
        self.log.info("♻️ Queues sorted by combined green efficiency (30% PUE / 70% CI weighting)")

        # 🌍 Show top-5 greenest queues
        top5 = queueDictItems[:5]
        self.log.always("🌍 Top 5 greenest queues this cycle:")
        for i, (_qname, qdata) in enumerate(top5, 1):
            site = qdata["Site"]
            pue = qdata["ParametersDict"].get("PUE", 0.0)
            ci = qdata["ParametersDict"].get("CI", 0.0)
            self.log.always(f"  {i}. {site:<25} PUE={pue:.2f}  CI={ci:.1f} gCO₂/kWh")

        # ---------------------------------------------------------------------
        # 🚀 Proceed with standard DIRAC pilot submission logic
        # ---------------------------------------------------------------------
        for queueName, queueDictionary in queueDictItems:
            self.log.verbose("Evaluating queue", queueName)

            # are we going to submit pilots to this specific queue?
            if not self._allowedToSubmit(queueName, anySite, jobSites, testSites):
                continue

            if "CPUTime" in queueDictionary["ParametersDict"]:
                queueCPUTime = int(queueDictionary["ParametersDict"]["CPUTime"])
            else:
                self.log.warn("CPU time limit is not specified, skipping", f"queue {queueName}")
                continue
            if queueCPUTime > self.maxQueueLength:
                queueCPUTime = self.maxQueueLength

            ce, ceDict = self._getCE(queueName)

            # additionalInfo is normally taskQueueDict
            pilotsWeMayWantToSubmit, additionalInfo = self._getPilotsWeMayWantToSubmit(ceDict)
            self.log.debug(f"{pilotsWeMayWantToSubmit} pilotsWeMayWantToSubmit are eligible for {queueName} queue")
            if not pilotsWeMayWantToSubmit:
                self.log.debug(f"...so skipping {queueName}")
                continue

            # Get the number of already waiting pilots for the queue
            totalWaitingPilots = 0
            manyWaitingPilotsFlag = False
            if self.pilotWaitingFlag:
                tqIDList = list(additionalInfo)
                result = self.pilotAgentsDB.countPilots(
                    {"TaskQueueID": tqIDList, "Status": PilotStatus.PILOT_WAITING_STATES}, None
                )
                if not result["OK"]:
                    self.log.error("Failed to get Number of Waiting pilots", result["Message"])
                    totalWaitingPilots = 0
                else:
                    totalWaitingPilots = result["Value"]
                    self.log.debug(f"Waiting Pilots: {totalWaitingPilots}")
            if totalWaitingPilots >= pilotsWeMayWantToSubmit:
                self.log.verbose("Possibly enough pilots already waiting", f"({totalWaitingPilots})")
                manyWaitingPilotsFlag = True
                if not self.addPilotsToEmptySites:
                    continue

            self.log.debug(
                f"{totalWaitingPilots} waiting pilots for the total of {pilotsWeMayWantToSubmit} eligible pilots for {queueName}"
            )

            # Get the number of available slots on the target site/queue
            totalSlots = self.getQueueSlots(queueName, manyWaitingPilotsFlag)
            if totalSlots <= 0:
                self.log.debug(f"{queueName}: No slots available")
                continue

            if manyWaitingPilotsFlag:
                # Throttle submission of extra pilots to empty sites
                pilotsToSubmit = int(self.maxPilotsToSubmit / 10) + 1
            else:
                pilotsToSubmit = max(0, min(totalSlots, pilotsWeMayWantToSubmit - totalWaitingPilots))
                self.log.info(
                    f"{queueName}: Slots={totalSlots}, TQ jobs(pilotsWeMayWantToSubmit)={pilotsWeMayWantToSubmit}, Pilots: waiting {totalWaitingPilots}, to submit={pilotsToSubmit}"
                )

            # Limit the number of pilots to submit to MAX_PILOTS_TO_SUBMIT
            pilotsToSubmit = min(self.maxPilotsToSubmit, pilotsToSubmit)

            # Get the working proxy
            cpuTime = queueCPUTime + 86400
            self.log.verbose("Getting pilot proxy", f"for {self.pilotDN}/{self.pilotGroup} {cpuTime} long")
            result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, cpuTime)
            if not result["OK"]:
                return result
            proxy = result["Value"]
            # Check returned proxy lifetime
            result = proxy.getRemainingSecs()  # pylint: disable=no-member
            if not result["OK"]:
                return result
            lifetime_secs = result["Value"]
            ce.setProxy(proxy, lifetime_secs)

            # Get valid token if needed
            if self.__supportToken(ce):
                result = self.__getPilotToken(audience=ce.audienceName)
                if not result["OK"]:
                    return result
                ce.setToken(result["Value"])

            # now really submitting
            res = self._submitPilotsToQueue(pilotsToSubmit, ce, queueName)
            if not res["OK"]:
                self.log.info("Failed pilot submission", f"Queue: {queueName}")
            else:
                pilotList, stampDict = res["Value"]

                # updating the pilotAgentsDB
                self._addPilotTQReference(queueName, additionalInfo, pilotList, stampDict)

        # Summary after the cycle over queues
        self.log.info("Total number of pilots submitted in this cycle", f"{self.totalSubmittedPilots}")

        return S_OK()

    # --------------------------------------------------------------------------
    # Token helpers
    # --------------------------------------------------------------------------
    def __supportToken(self, ce: ComputingElement) -> bool:
        """Check whether the SiteDirector is able to submit pilots with tokens."""
        return "Token" in ce.ceParameters.get("Tag", []) or f"Token:{self.vo}" in ce.ceParameters.get("Tag", [])

    def __getPilotToken(self, audience: str, scope: list[str] | None = None):
        """Get the token corresponding to the pilot user identity

        :param audience: Token audience, targeting a single CE
        :param scope: list of permissions needed to interact with a CE
        :return: S_OK/S_ERROR, Token object as Value
        """
        if not audience:
            return S_ERROR("Audience is not defined")

        if not scope:
            scope = PILOT_SCOPES

        return gTokenManager.getToken(userGroup=self.pilotGroup, requiredTimeLeft=600, scope=scope, audience=audience)

    # --------------------------------------------------------------------------
    # Matching helpers
    # --------------------------------------------------------------------------
    def _ifAndWhereToSubmit(self):
        """Return a tuple that says if and where to submit pilots:

        (submit, anySite, jobSites, testSites)
        """
        tqDict = self._getTQDictForMatching()
        if not tqDict:
            return True, True, set(), set()

        # the tqDict used here is a very generic one, not specific to one CE/queue only
        self.log.verbose("Checking overall TQ availability with requirements")
        self.log.verbose(tqDict)

        # Check that there is some work at all
        result = self.matcherClient.getMatchingTaskQueues(tqDict)
        if not result["OK"]:
            self.log.error("Matcher error: ", result["Message"])
            return False, True, set(), set()
        matchingTQs = result["Value"]
        if not matchingTQs:
            self.log.notice("No Waiting jobs suitable for the director, so nothing to submit")
            return False, True, set(), set()

        # If we are here there's some work to do, now let's see for where
        jobSites = set()
        testSites = set()
        anySite = False

        for tqDescription in matchingTQs.values():
            siteList = tqDescription.get("Sites", [])
            if siteList:
                jobSites |= set(siteList)
            else:
                anySite = True

            if "JobTypes" in tqDescription:
                if "Sites" in tqDescription:
                    for site in tqDescription["Sites"]:
                        if site.lower() != "any":
                            testSites.add(site)

        self.monitorJobsQueuesPilots(matchingTQs)

        return True, anySite, jobSites, testSites

    def monitorJobsQueuesPilots(self, matchingTQs):
        """Printout of jobs, queues and pilots status in TQ"""
        tqIDList = list(matchingTQs)
        result = self.pilotAgentsDB.countPilots(
            {"TaskQueueID": tqIDList, "Status": PilotStatus.PILOT_WAITING_STATES}, None
        )

        totalWaitingJobs = 0
        for tqDescription in matchingTQs.values():
            totalWaitingJobs += tqDescription["Jobs"]

        if not result["OK"]:
            self.log.error("Can't count pilots", result["Message"])
        else:
            self.log.info(
                "Total jobs : number of task queues : number of waiting pilots",
                f"{totalWaitingJobs} : {len(tqIDList)} : {result['Value']}",
            )

    def _getTQDictForMatching(self):
        """Construct a dictionary (tqDict) used to check with Matcher if there's anything to submit."""
        tqDict = {"Setup": CSGlobals.getSetup(), "CPUTime": 9999999}
        if self.vo:
            tqDict["Community"] = self.vo
        if self.voGroups:
            tqDict["OwnerGroup"] = self.voGroups

        if self.checkPlatform:
            platforms = self._getPlatforms()
            if platforms:
                tqDict["Platform"] = platforms

        tqDict["Site"] = self.sites

        # Get a union of all tags
        tags = []
        for queue in self.queueDict:
            tags += self.queueDict[queue]["ParametersDict"].get("Tag", [])
        tqDict["Tag"] = list(set(tags))

        # Add overall max values for all queues
        tqDict.update(self.globalParameters)

        return tqDict

    def _getPlatforms(self):
        """Get the platforms used for TQ match (extension point)."""
        result = self.resourcesModule.getCompatiblePlatforms(self.platforms)
        if not result["OK"]:
            self.log.error(
                "Issue getting compatible platforms, will skip check of platforms",
                self.platforms + " : " + result["Message"],
            )
        return result["Value"]

    def _allowedToSubmit(self, queue, anySite, jobSites, testSites):
        """Check if we are allowed to submit to a certain queue"""

        # Check if the queue failed previously
        failedCount = self.failedQueues[queue] % self.failedQueueCycleFactor
        if failedCount != 0:
            self.log.warn("queue failed recently ==> number of cycles skipped", f"{queue} ==> {10 - failedCount}")
            self.failedQueues[queue] += 1
            return False

        # Check the status of the site
        if self.queueDict[queue]["Site"] not in self.siteMaskList and self.queueDict[queue]["Site"] not in testSites:
            self.log.verbose(
                "Queue skipped (site not in mask)",
                f"{self.queueDict[queue]['QueueName']} ({self.queueDict[queue]['Site']})",
            )
            return False

        # Check that there are task queues waiting for this site
        if not anySite and self.queueDict[queue]["Site"] not in jobSites:
            self.log.verbose(
                "Queue skipped: no workload expected",
                f"{self.queueDict[queue]['CEName']} at {self.queueDict[queue]['Site']}",
            )
            return False

        # Check the status of the CE (only for RSS=Active)
        if self.rssFlag:
            if self.queueDict[queue]["CEName"] not in self.ceMaskList:
                self.log.verbose(
                    "Skipping computing element: resource not usable",
                    f"{self.queueDict[queue]['CEName']} at {self.queueDict[queue]['Site']}",
                )
                return False

        # if we are here, it means that we are allowed to submit to the queue
        return True

    # --------------------------------------------------------------------------
    # CE helpers
    # --------------------------------------------------------------------------
    def _getCE(self, queue):
        """Prepare the queue description to look for eligible jobs"""
        ce = self.queueDict[queue]["CE"]
        ceDict = ce.ceParameters
        ceDict["GridCE"] = self.queueDict[queue]["CEName"]

        if self.queueDict[queue]["Site"] not in self.siteMaskList:
            ceDict["JobType"] = "Test"
        if self.vo:
            ceDict["Community"] = self.vo
        if self.voGroups:
            ceDict["OwnerGroup"] = self.voGroups

        if self.checkPlatform:
            platform = self.queueDict[queue]["ParametersDict"].get("Platform")
            if not platform:
                self.log.error(f"No platform set for CE {ce}, returning 'ANY'")
                ceDict["Platform"] = "ANY"
                return ce, ceDict
            result = self.resourcesModule.getCompatiblePlatforms(platform)
            if result["OK"]:
                ceDict["Platform"] = result["Value"]
            else:
                self.log.error(
                    "Issue getting compatible platforms, returning 'ANY'", f"{self.platforms}: {result['Message']}"
                )
                ceDict["Platform"] = "ANY"

        return ce, ceDict

    def _getPilotsWeMayWantToSubmit(self, ceDict):
        """Returns the number of pilots we may want to submit to the ce described in ceDict."""
        pilotsWeMayWantToSubmit = 0

        result = self.matcherClient.getMatchingTaskQueues(ceDict)
        if not result["OK"]:
            self.log.error("Could not retrieve TaskQueues from TaskQueueDB", result["Message"])
            return 0, {}
        taskQueueDict = result["Value"]
        if not taskQueueDict:
            self.log.verbose("No matching TQs found", f"for {ceDict}")

        for tq in taskQueueDict.values():
            pilotsWeMayWantToSubmit += tq["Jobs"]

        return pilotsWeMayWantToSubmit, taskQueueDict

    def _submitPilotsToQueue(self, pilotsToSubmit, ce, queue):
        """Actually submit the pilots to the CE's queue"""
        self.log.info("Going to submit pilots", f"(a maximum of {pilotsToSubmit} pilots to {queue} queue)")

        jobExecDir = self.queueDict[queue]["ParametersDict"].get("JobExecDir", "")
        envVariables = self.queueDict[queue]["ParametersDict"].get("EnvironmentVariables", None)

        # We don't want to use the submission/"pilot" proxy for the job in the bundle:
        # Instead we use a non-VOMS proxy which is then not limited in lifetime by the VOMS extension
        proxyTimeSec = int(self.maxQueueLength * self.bundleProxyLifetimeFactor)
        result = gProxyManager.downloadProxy(self.pilotDN, self.pilotGroup, limited=True, requiredTimeLeft=proxyTimeSec)
        if not result["OK"]:
            self.log.error("Failed to get job proxy", f"Queue {queue}:\n{result['Message']}")
            return result
        jobProxy = result["Value"]

        executable = self.getExecutable(queue, proxy=jobProxy, jobExecDir=jobExecDir, envVariables=envVariables)

        submitResult = ce.submitJob(executable, "", pilotsToSubmit)
        # In case the CE does not need the executable after the submission, we delete it
        # Else, we keep it, the CE will delete it after the end of the pilot execution
        if submitResult.get("ExecutableToKeep") != executable:
            try:
                os.unlink(executable)
            except Exception:
                pass

        if not submitResult["OK"]:
            self.log.error("Failed submission to queue", f"Queue {queue}:\n{submitResult['Message']}")

            if self.sendSubmissionAccounting:
                result = self.sendPilotSubmissionAccounting(
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["CEName"],
                    self.queueDict[queue]["QueueName"],
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    self.log.error("Failure submitting Accounting report", result["Message"])

            if self.sendSubmissionMonitoring:
                result = self.sendPilotSubmissionMonitoring(
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["CEName"],
                    self.queueDict[queue]["QueueName"],
                    pilotsToSubmit,
                    0,
                    "Failed",
                )
                if not result["OK"]:
                    self.log.error("Failure submitting Monitoring report", result["Message"])

            self.failedQueues[queue] += 1
            return submitResult

        # Add pilots to the PilotAgentsDB: assign pilots to TaskQueue proportionally to the task queue priorities
        pilotList = submitResult["Value"]
        self.queueSlots[queue]["AvailableSlots"] -= len(pilotList)

        self.totalSubmittedPilots += len(pilotList)
        self.log.info(
            f"Submitted {len(pilotList)} pilots to {self.queueDict[queue]['QueueName']}@{self.queueDict[queue]['CEName']}"
        )
        stampDict = submitResult.get("PilotStampDict", {})
        if self.sendSubmissionAccounting:
            result = self.sendPilotSubmissionAccounting(
                self.queueDict[queue]["Site"],
                self.queueDict[queue]["CEName"],
                self.queueDict[queue]["QueueName"],
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                self.log.error("Failure submitting Accounting report", result["Message"])

        if self.sendSubmissionMonitoring:
            result = self.sendPilotSubmissionMonitoring(
                self.queueDict[queue]["Site"],
                self.queueDict[queue]["CEName"],
                self.queueDict[queue]["QueueName"],
                len(pilotList),
                len(pilotList),
                "Succeeded",
            )
            if not result["OK"]:
                self.log.error("Failure submitting Monitoring report", result["Message"])

        return S_OK((pilotList, stampDict))

    def _addPilotTQReference(self, queue, taskQueueDict, pilotList, stampDict):
        """Add to pilotAgentsDB the reference for which TqID the pilots have been sent"""
        tqPriorityList = []
        sumPriority = 0.0
        for tq in taskQueueDict:
            sumPriority += taskQueueDict[tq]["Priority"]
            tqPriorityList.append((tq, sumPriority))
        tqDict = {}
        for pilotID in pilotList:
            rndm = random.random() * sumPriority
            tqID = None
            for tq, prio in tqPriorityList:
                if rndm < prio:
                    tqID = tq
                    break
            if tqID is None:
                continue
            if tqID not in tqDict:
                tqDict[tqID] = []
            tqDict[tqID].append(pilotID)

        for tqID, pilotsList in tqDict.items():
            result = self.pilotAgentsDB.addPilotTQReference(
                pilotsList,
                tqID,
                self.pilotDN,
                self.pilotGroup,
                self.localhost,
                self.queueDict[queue]["CEType"],
                stampDict,
            )
            if not result["OK"]:
                self.log.error("Failed add pilots to the PilotAgentsDB", result["Message"])
                continue
            for pilot in pilotsList:
                result = self.pilotAgentsDB.setPilotStatus(
                    pilot,
                    PilotStatus.SUBMITTED,
                    self.queueDict[queue]["CEName"],
                    "Successfully submitted by the SiteDirector",
                    self.queueDict[queue]["Site"],
                    self.queueDict[queue]["QueueName"],
                )
                if not result["OK"]:
                    self.log.error("Failed to set pilot status", result["Message"])
                    continue

    # --------------------------------------------------------------------------
    # Queue slots & pilot output helpers
    # --------------------------------------------------------------------------
    def getQueueSlots(self, queue, manyWaitingPilotsFlag):
        """Get the number of available slots in the queue"""
        ce = self.queueDict[queue]["CE"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]
        queryCEFlag = self.queueDict[queue]["QueryCEFlag"].lower() in ["1", "yes", "true"]

        self.queueSlots.setdefault(queue, {})
        totalSlots = self.queueSlots[queue].get("AvailableSlots", 0)

        # See if there are waiting pilots for this queue. If not, allow submission
        if totalSlots and manyWaitingPilotsFlag:
            result = self.pilotAgentsDB.selectPilots(
                {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_WAITING_STATES}
            )
            if result["OK"]:
                jobIDList = result["Value"]
                if not jobIDList:
                    return totalSlots
            return 0

        availableSlotsCount = self.queueSlots[queue].setdefault("AvailableSlotsCount", 0)
        waitingJobs = 1
        if totalSlots == 0:
            if availableSlotsCount % self.availableSlotsUpdateCycleFactor == 0:
                # Get the list of already existing pilots for this queue
                jobIDList = None
                result = self.pilotAgentsDB.selectPilots(
                    {"DestinationSite": ceName, "Queue": queueName, "Status": PilotStatus.PILOT_TRANSIENT_STATES}
                )

                if result["OK"]:
                    jobIDList = result["Value"]

                if queryCEFlag:
                    result = ce.available(jobIDList)
                    if not result["OK"]:
                        self.log.warn("Failed to check the availability of queue", f"{queue}: \n{result['Message']}")
                        self.failedQueues[queue] += 1
                    else:
                        ceInfoDict = result["CEInfoDict"]
                        self.log.info(
                            "CE queue report",
                            f"({ceName}_{queueName}): Wait={ceInfoDict['WaitingJobs']}, Run={ceInfoDict['RunningJobs']}, Submitted={ceInfoDict['SubmittedJobs']}, Max={ceInfoDict['MaxTotalJobs']}",
                        )
                        totalSlots = result["Value"]
                        self.queueSlots[queue]["AvailableSlots"] = totalSlots
                        waitingJobs = ceInfoDict["WaitingJobs"]
                else:
                    maxWaitingJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxWaitingJobs", 10))
                    maxTotalJobs = int(self.queueDict[queue]["ParametersDict"].get("MaxTotalJobs", 10))
                    waitingToRunningRatio = float(
                        self.queueDict[queue]["ParametersDict"].get("WaitingToRunningRatio", 0.0)
                    )
                    waitingJobs = 0
                    totalJobs = 0
                    if jobIDList:
                        result = self.pilotAgentsDB.getPilotInfo(jobIDList)
                        if not result["OK"]:
                            self.log.warn("Failed to check PilotAgentsDB", f"for queue {queue}: \n{result['Message']}")
                            self.failedQueues[queue] += 1
                        else:
                            for _pilotRef, pilotDict in result["Value"].items():
                                if pilotDict["Status"] in PilotStatus.PILOT_TRANSIENT_STATES:
                                    totalJobs += 1
                                    if pilotDict["Status"] in PilotStatus.PILOT_WAITING_STATES:
                                        waitingJobs += 1
                            runningJobs = totalJobs - waitingJobs
                            self.log.info(
                                "PilotAgentsDB report",
                                f"({ceName}_{queueName}): Wait={waitingJobs}, Run={runningJobs}, Max={maxTotalJobs}",
                            )
                            maxWaitingJobs = int(max(maxWaitingJobs, runningJobs * waitingToRunningRatio))

                    totalSlots = min((maxTotalJobs - totalJobs), (maxWaitingJobs - waitingJobs))
                    self.queueSlots[queue]["AvailableSlots"] = max(totalSlots, 0)

        self.queueSlots[queue]["AvailableSlotsCount"] += 1

        if manyWaitingPilotsFlag and waitingJobs:
            return 0
        return totalSlots

    def getExecutable(self, queue, proxy=None, jobExecDir="", envVariables=None, **kwargs):
        """Prepare the full executable for queue"""

        pilotOptions = self._getPilotOptions(queue, **kwargs)
        if not pilotOptions:
            self.log.warn("Pilots will be submitted without additional options")
            pilotOptions = []
        pilotOptions = " ".join(pilotOptions)
        self.log.verbose(f"pilotOptions: {pilotOptions}")

        # if a global workingDirectory is defined for the CEType (like HTCondor) use it
        ce = self.queueCECache[queue]["CE"]
        workingDirectory = getattr(ce, "workingDirectory", self.workingDirectory)

        executable = self._writePilotScript(
            workingDirectory=workingDirectory,
            pilotOptions=pilotOptions,
            proxy=proxy,
            pilotExecDir=jobExecDir,
            envVariables=envVariables,
        )
        return executable

    def _getPilotOptions(self, queue, **kwargs):
        """Prepare pilot options for dirac-pilot invocation"""
        queueDict = self.queueDict[queue]["ParametersDict"]
        pilotOptions = []

        setup = gConfig.getValue("/DIRAC/Setup", "unknown")
        if setup == "unknown":
            self.log.error("Setup is not defined in the configuration")
            return [None, None]
        pilotOptions.append(f"-S {setup}")
        opsHelper = Operations(group=self.pilotGroup, setup=setup)

        # Installation defined?
        installationName = opsHelper.getValue("Pilot/Installation", "")
        if installationName:
            pilotOptions.append(f"-V {installationName}")

        # Project defined?
        projectName = opsHelper.getValue("Pilot/Project", "")
        if projectName:
            pilotOptions.append(f"-l {projectName}")
        else:
            self.log.info("DIRAC project will be installed by pilots")

        # Architecture script to use
        architectureScript = opsHelper.getValue("Pilot/ArchitectureScript", "")
        if architectureScript:
            pilotOptions.append(f"--architectureScript={architectureScript}")

        # Preinstalled environment or list of CVMFS locations defined ?
        preinstalledEnv = opsHelper.getValue("Pilot/PreinstalledEnv", "")
        preinstalledEnvPrefix = opsHelper.getValue("Pilot/PreinstalledEnvPrefix", "")
        CVMFS_locations = opsHelper.getValue("Pilot/CVMFS_locations", "")
        if preinstalledEnv:
            pilotOptions.append(f"--preinstalledEnv={preinstalledEnv}")
        elif preinstalledEnvPrefix:
            pilotOptions.append(f"--preinstalledEnvPrefix={preinstalledEnvPrefix}")
        elif CVMFS_locations:
            pilotOptions.append(f"--CVMFS_locations={CVMFS_locations}")

        # Pilot Logging defined?
        if opsHelper.getValue("/Services/JobMonitoring/usePilotsLoggingFlag", False):
            pilotOptions.append("-z ")

        # Debug
        if self.pilotLogLevel.lower() == "debug":
            self.log.debug("Pilot debug enabled")
            pilotOptions.append("-ddd")

        # DIRAC Extensions to be used in pilots
        pilotExtensionsList = opsHelper.getValue("Pilot/Extensions", [])
        if pilotExtensionsList and pilotExtensionsList[0] != "None":
            extensionsList = pilotExtensionsList
        else:
            extensionsList = [ext for ext in CSGlobals.getCSExtensions() if "Web" not in ext]
        if extensionsList:
            pilotOptions.append(f"-e {','.join(extensionsList)}")

        # CEName / Queue / SiteName
        pilotOptions.append(f"-N {self.queueDict[queue]['CEName']}")
        pilotOptions.append(f"-Q {self.queueDict[queue]['QueueName']}")
        pilotOptions.append(f"-n {queueDict['Site']}")

        # VO
        if self.vo:
            pilotOptions.append(f"--wnVO={self.vo}")

        # Generic Options
        if "GenericOptions" in queueDict:
            for genericOption in queueDict["GenericOptions"].split(","):
                pilotOptions.append(f"-o {genericOption.strip()}")

        if "SharedArea" in queueDict:
            pilotOptions.append(f"-o '/LocalSite/SharedArea={queueDict['SharedArea']}'")

        if "UserEnvVariables" in queueDict:
            pilotOptions.append(f"--userEnvVariables={queueDict['UserEnvVariables']}")

        if "ExtraPilotOptions" in queueDict:
            for extraPilotOption in queueDict["ExtraPilotOptions"].split(","):
                pilotOptions.append(extraPilotOption.strip())

        if "Modules" in queueDict:
            pilotOptions.append(f"--modules={queueDict['Modules']}")

        if "PipInstallOptions" in queueDict:
            pilotOptions.append(f"--pipInstallOptions={queueDict['PipInstallOptions']}")

        if self.group:
            pilotOptions.append(f"-G {self.group}")

        return pilotOptions

    def _writePilotScript(self, workingDirectory, pilotOptions, proxy, pilotExecDir="", envVariables=None):
        """Bundle together and write out the pilot executable script, admix the proxy if given"""
        try:
            pilotFilesCompressedEncodedDict = getPilotFilesCompressedEncodedDict([], proxy)
        except Exception as be:
            self.log.exception("Exception during pilot modules files compression", lException=be)
            pilotFilesCompressedEncodedDict = {}

        location = Operations().getValue("Pilot/pilotFileServer", "")
        CVMFS_locations = Operations().getValue("Pilot/CVMFS_locations", [])

        localPilot = pilotWrapperScript(
            pilotFilesCompressedEncodedDict=pilotFilesCompressedEncodedDict,
            pilotOptions=pilotOptions,
            pilotExecDir=pilotExecDir,
            envVariables=envVariables,
            location=location,
            CVMFS_locations=CVMFS_locations,
        )

        return _writePilotWrapperFile(workingDirectory=workingDirectory, localPilot=localPilot)

    # --------------------------------------------------------------------------
    # Pilot status / output / accounting
    # --------------------------------------------------------------------------
    def updatePilotStatus(self):
        """Update status of pilots in transient and final states"""

        # Generate a proxy before feeding the threads to renew the ones of the CEs to perform actions
        result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, 23400)
        if not result["OK"]:
            return result
        proxy = result["Value"]

        # Threads to update pilot status in transient states
        with ThreadPoolExecutor(max_workers=len(self.queueDict)) as executor:
            futures = []
            for queue in self.queueDict:
                futures.append(executor.submit(self._updatePilotStatusPerQueue, queue, proxy))
            for res in as_completed(futures):
                err = res.exception()
                if err:
                    self.log.exception("Update pilot status thread failed", lException=err)

        # The pilot can be in Done state set by the job agent; check if the output is retrieved
        for queue in self.queueDict:
            ce = self.queueDict[queue]["CE"]

            if not ce.isProxyValid(120)["OK"]:
                result = gProxyManager.getPilotProxyFromDIRACGroup(self.pilotDN, self.pilotGroup, 1000)
                if not result["OK"]:
                    return result
                proxy = result["Value"]
                ce.setProxy(proxy, 940)

            if callable(getattr(ce, "cleanupPilots", None)):
                ce.cleanupPilots()

            ceName = self.queueDict[queue]["CEName"]
            queueName = self.queueDict[queue]["QueueName"]
            ceType = self.queueDict[queue]["CEType"]
            siteName = self.queueDict[queue]["Site"]
            result = self.pilotAgentsDB.selectPilots(
                {
                    "DestinationSite": ceName,
                    "Queue": queueName,
                    "GridType": ceType,
                    "GridSite": siteName,
                    "OutputReady": "False",
                    "Status": PilotStatus.PILOT_FINAL_STATES,
                }
            )

            if not result["OK"]:
                self.log.error("Failed to select pilots", result["Message"])
                continue
            pilotRefs = result["Value"]
            if not pilotRefs:
                continue
            result = self.pilotAgentsDB.getPilotInfo(pilotRefs)
            if not result["OK"]:
                self.log.error("Failed to get pilots info from DB", result["Message"])
                continue
            pilotDict = result["Value"]
            if self.getOutput:
                for pRef in pilotRefs:
                    self._getPilotOutput(pRef, pilotDict, ce, ceName)

            # Check if the accounting is to be sent
            if self.sendAccounting:
                result = self.pilotAgentsDB.selectPilots(
                    {
                        "DestinationSite": ceName,
                        "Queue": queueName,
                        "GridType": ceType,
                        "GridSite": siteName,
                        "AccountingSent": "False",
                        "Status": PilotStatus.PILOT_FINAL_STATES,
                    }
                )

                if not result["OK"]:
                    self.log.error("Failed to select pilots", result["Message"])
                    continue
                pilotRefs = result["Value"]
                if not pilotRefs:
                    continue
                result = self.pilotAgentsDB.getPilotInfo(pilotRefs)
                if not result["OK"]:
                    self.log.error("Failed to get pilots info from DB", result["Message"])
                    continue
                pilotDict = result["Value"]
                result = self.sendPilotAccounting(pilotDict)
                if not result["OK"]:
                    self.log.error("Failed to send pilot agent accounting")

        return S_OK()

    def _updatePilotStatusPerQueue(self, queue, proxy):
        """Update status of pilots in transient state for a given queue"""
        ce = self.queueDict[queue]["CE"]
        ceName = self.queueDict[queue]["CEName"]
        queueName = self.queueDict[queue]["QueueName"]
        ceType = self.queueDict[queue]["CEType"]
        siteName = self.queueDict[queue]["Site"]

        result = self.pilotAgentsDB.selectPilots(
            {
                "DestinationSite": ceName,
                "Queue": queueName,
                "GridType": ceType,
                "GridSite": siteName,
                "Status": PilotStatus.PILOT_TRANSIENT_STATES,
                "OwnerDN": self.pilotDN,
                "OwnerGroup": self.pilotGroup,
            }
        )
        if not result["OK"]:
            self.log.error("Failed to select pilots", f": {result['Message']}")
            return
        pilotRefs = result["Value"]
        if not pilotRefs:
            return

        result = self.pilotAgentsDB.getPilotInfo(pilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots info from DB", result["Message"])
            return
        pilotDict = result["Value"]

        # Ensure the CE has a long-enough proxy
        result = ce.isProxyValid(3 * 3600)
        if not result["OK"]:
            ce.setProxy(proxy, 23300)

        # Token if needed
        if self.__supportToken(ce):
            result = self.__getPilotToken(audience=ce.audienceName)
            if not result["OK"]:
                self.log.error("Failed to get token", f"{ceName}: {result['Message']}")
                return
            ce.setToken(result["Value"])

        result = ce.getJobStatus(pilotRefs)
        if not result["OK"]:
            self.log.error("Failed to get pilots status from CE", f"{ceName}: {result['Message']}")
            return
        pilotCEDict = result["Value"]

        abortedPilots, getPilotOutput = self._updatePilotStatus(pilotRefs, pilotDict, pilotCEDict)
        for pRef in getPilotOutput:
            self._getPilotOutput(pRef, pilotDict, ce, ceName)

        # If something wrong in the queue, make a pause for the job submission
        if abortedPilots:
            self.failedQueues[queue] += 1

    def _updatePilotStatus(self, pilotRefs, pilotDict, pilotCEDict):
        """Really updates the pilots status

        :return: number of aborted pilots, flag for getting the pilot output
        """

        abortedPilots = 0
        getPilotOutput = []

        for pRef in pilotRefs:
            newStatus = ""
            oldStatus = pilotDict[pRef]["Status"]
            lastUpdateTime = pilotDict[pRef]["LastUpdateTime"]
            sinceLastUpdate = datetime.utcnow() - lastUpdateTime

            ceStatus = pilotCEDict.get(pRef, oldStatus)

            if oldStatus == ceStatus and ceStatus != PilotStatus.UNKNOWN:
                # Normal status did not change, continue
                continue
            if ceStatus == oldStatus == PilotStatus.UNKNOWN:
                if sinceLastUpdate < 3600 * second:
                    # Allow 1 hour of Unknown status assuming temporary problems on the CE
                    continue
                newStatus = PilotStatus.ABORTED
            elif ceStatus == PilotStatus.UNKNOWN and oldStatus not in PilotStatus.PILOT_FINAL_STATES:
                # Possible problems on the CE, keep Unknown temporarily
                newStatus = PilotStatus.UNKNOWN
            elif ceStatus != PilotStatus.UNKNOWN:
                # Update the pilot status to the new value
                newStatus = ceStatus

            if newStatus:
                self.log.info("Updating status", f"to {newStatus} for pilot {pRef}")
                result = self.pilotAgentsDB.setPilotStatus(pRef, newStatus, "", "Updated by SiteDirector")
                if not result["OK"]:
                    self.log.error(result["Message"])
                if newStatus == "Aborted":
                    abortedPilots += 1
            # Set the flag to retrieve the pilot output now or not
            if newStatus in PilotStatus.PILOT_FINAL_STATES:
                if pilotDict[pRef]["OutputReady"].lower() == "false" and self.getOutput:
                    getPilotOutput.append(pRef)

        return abortedPilots, getPilotOutput

    def _getPilotOutput(self, pRef, pilotDict, ce, ceName):
        """Retrieves the pilot output for a pilot and stores it in the pilotAgentsDB"""
        self.log.info(f"Retrieving output for pilot {pRef}")
        output = None
        error = None

        pilotStamp = pilotDict[pRef]["PilotStamp"]
        pRefStamp = pRef
        if pilotStamp:
            pRefStamp = pRef + ":::" + pilotStamp

        result = ce.getJobOutput(pRefStamp)
        if not result["OK"]:
            self.failedPilotOutput[pRefStamp] += 1
            self.log.error("Failed to get pilot output", f"{ceName}: {result['Message']}")
            self.log.verbose(f"Retries left: {max(0, self.maxRetryGetPilotOutput - self.failedPilotOutput[pRefStamp])}")

            if (self.maxRetryGetPilotOutput - self.failedPilotOutput[pRefStamp]) <= 0:
                output = "Output is no longer available"
                error = "Error is no longer available"
                self.failedPilotOutput.pop(pRefStamp)
            else:
                return
        else:
            output, error = result["Value"]

        if output:
            result = self.pilotAgentsDB.storePilotOutput(pRef, output, error)
            if not result["OK"]:
                self.log.error("Failed to store pilot output", result["Message"])
        else:
            self.log.warn("Empty pilot output not stored to PilotDB")

    # --------------------------------------------------------------------------
    # Accounting / Monitoring
    # --------------------------------------------------------------------------
    def sendPilotAccounting(self, pilotDict):
        """Send pilot accounting record"""
        for pRef in pilotDict:
            self.log.verbose("Preparing accounting record", f"for pilot {pRef}")
            pA = PilotAccounting()
            pA.setEndTime(pilotDict[pRef]["LastUpdateTime"])
            pA.setStartTime(pilotDict[pRef]["SubmissionTime"])
            retVal = Registry.getUsernameForDN(pilotDict[pRef]["OwnerDN"])
            if not retVal["OK"]:
                username = "unknown"
                self.log.error("Can't determine username for dn", pilotDict[pRef]["OwnerDN"])
            else:
                username = retVal["Value"]
            pA.setValueByKey("User", username)
            pA.setValueByKey("UserGroup", pilotDict[pRef]["OwnerGroup"])
            result = getCESiteMapping(pilotDict[pRef]["DestinationSite"])
            if result["OK"] and result["Value"]:
                pA.setValueByKey("Site", result["Value"][pilotDict[pRef]["DestinationSite"]].strip())
            else:
                pA.setValueByKey("Site", "Unknown")
            pA.setValueByKey("GridCE", pilotDict[pRef]["DestinationSite"])
            pA.setValueByKey("GridMiddleware", pilotDict[pRef]["GridType"])
            pA.setValueByKey("GridResourceBroker", pilotDict[pRef]["Broker"])
            pA.setValueByKey("GridStatus", pilotDict[pRef]["Status"])
            if "Jobs" not in pilotDict[pRef]:
                pA.setValueByKey("Jobs", 0)
            else:
                pA.setValueByKey("Jobs", len(pilotDict[pRef]["Jobs"]))
            self.log.verbose("Adding accounting record", f"for pilot {pilotDict[pRef]['PilotID']}")
            retVal = gDataStoreClient.addRegister(pA)
            if not retVal["OK"]:
                self.log.error("Failed to send accounting info for pilot ", pRef)
            else:
                # Set up AccountingSent flag
                result = self.pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)

        self.log.info("Committing accounting records", f"for {len(pilotDict)} pilots")
        result = gDataStoreClient.commit()
        if result["OK"]:
            for pRef in pilotDict:
                self.log.verbose("Setting AccountingSent flag", f"for pilot {pRef}")
                result = self.pilotAgentsDB.setAccountingFlag(pRef)
                if not result["OK"]:
                    self.log.error("Failed to set accounting flag for pilot ", pRef)
        else:
            return result

        return S_OK()

    def sendPilotSubmissionAccounting(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
        """Send pilot submission accounting record"""
        pA = PilotSubmissionAccounting()
        pA.setStartTime(datetime.utcnow())
        pA.setEndTime(datetime.utcnow())
        pA.setValueByKey("HostName", DIRAC.siteName())
        if hasattr(self, "_AgentModule__moduleProperties"):
            pA.setValueByKey("SiteDirector", self.am_getModuleParam("agentName"))
        else:  # In case it is not executed as agent
            pA.setValueByKey("SiteDirector", "Client")

        pA.setValueByKey("Site", siteName)
        pA.setValueByKey("CE", ceName)
        pA.setValueByKey("Queue", ceName + ":" + queueName)
        pA.setValueByKey("Status", status)
        pA.setValueByKey("NumTotal", numTotal)
        pA.setValueByKey("NumSucceeded", numSucceeded)
        result = gDataStoreClient.addRegister(pA)

        if not result["OK"]:
            self.log.warn("Error in add Register:" + result["Message"])
            return result

        self.log.verbose("Committing pilot submission to accounting")
        result = gDataStoreClient.delayedCommit()
        if not result["OK"]:
            self.log.error("Could not commit pilot submission to accounting", result["Message"])
            return result
        return S_OK()

    def sendPilotSubmissionMonitoring(self, siteName, ceName, queueName, numTotal, numSucceeded, status):
        """Sends pilot submission records to monitoring"""
        pilotMonitoringReporter = MonitoringReporter(monitoringType="PilotSubmissionMonitoring")

        if hasattr(self, "_AgentModule__moduleProperties"):
            siteDirName = self.am_getModuleParam("agentName")
        else:  # In case it is not executed as agent
            siteDirName = "Client"

        pilotMonitoringData = {
            "HostName": DIRAC.siteName(),
            "SiteDirector": siteDirName,
            "Site": siteName,
            "CE": ceName,
            "Queue": ceName + ":" + queueName,
            "Status": status,
            "NumTotal": numTotal,
            "NumSucceeded": numSucceeded,
            "timestamp": int(toEpochMilliSeconds(datetime.utcnow())),
        }
        pilotMonitoringReporter.addRecord(pilotMonitoringData)

        self.log.verbose("Committing pilot submission to monitoring")
        result = pilotMonitoringReporter.commit()
        if not result["OK"]:
            self.log.error("Could not commit pilot submission to monitoring", result["Message"])
            return S_ERROR()
        self.log.verbose("Done committing to monitoring")
        return S_OK()
