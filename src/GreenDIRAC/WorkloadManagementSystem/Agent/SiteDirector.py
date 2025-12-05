#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GreenSiteDirector — READ-ONLY green-aware SiteDirector

New metric introduced:
    GREEN_SCORE = CEE / (CI * PUE)

Higher GREEN_SCORE => Greener queue.

Differences from original:
  ✔ READ-ONLY (NO write to CS)
  ✔ Supports CIM/CI API + CE-level CS fallback
  ✔ Computes green score as CEE/(CI*PUE)
  ✔ Sorts queueDict accordingly
  ✔ Includes dictionary layouts at each step
"""

from __future__ import annotations    # <— Python 3: needed to allow forward type annotations

import time
from datetime import datetime, timezone, timedelta
import requests
from DIRAC import S_OK, gConfig

# Base class
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as BaseSiteDirector

# Helper for CE→GOCDB mapping
from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary


# =====================================================================
# CONSTANTS
# =====================================================================
DEFAULT_PUE = 5.0
DEFAULT_CI = 9999.0
DEFAULT_CEE = 1.0

ENERGY_WH_DEFAULT = 8500

GREEN_CACHE_TTL = 180
TOKEN_MAX_AGE_H = 24.0


# =====================================================================
# GREEN SITE DIRECTOR — READ ONLY
# =====================================================================
class SiteDirector(BaseSiteDirector):

    def __init__(self, *args, **kwargs):
        super(SiteDirector, self).__init__(*args, **kwargs)
        self.log.always("🌿 GreenSiteDirector (READ-ONLY) loaded.")

        # CIM API config
        self.cim_api_base = self.am_getOption("CIM_API_BASE", "").strip()
        self.ci_api_base  = self.am_getOption("CI_API_BASE", "").strip()
        self.cim_email    = self.am_getOption("CIM_EMAIL", "").strip()
        self.cim_password = self.am_getOption("CIM_PASSWORD", "").strip()

        # cache
        self._token = None
        self._token_ts = None
        self.green_cache = {}     # (site,ce) → (timestamp, PUE, CI)

    # =================================================================
    # BEGIN EXECUTION
    # =================================================================
    def beginExecution(self):
        """
        Steps:
          1. Get queueDict from parent
          2. For each queue:
               - obtain PUE/CI
               - obtain CEE from CE-level CS if available
               - compute GREEN_SCORE = CEE/(CI*PUE)
               - store inside queueDict
          3. Sort queueDict by new GREEN_SCORE (descending)
        """

        result = super(SiteDirector, self).beginExecution()
        if not result["OK"]:
            return result

        # Example layout of queueDict (debug)
        # {
        #   'QueueName1': {
        #         'Site': 'EGI.SARA.nl',
        #         'CEName': 'arc03.gina',
        #         'ParametersDict': {...}
        #   }
        #   ...
        # }
        self._debugFirstQueues()

        if not self.queueDict:
            return S_OK()

        # ---------------------------------------------------------
        # Enrich queueDict with (PUE, CI, CEE, GREEN_SCORE)
        # ---------------------------------------------------------
        for qName, qDict in self.queueDict.items():

            site  = qDict.get("Site", "")
            ce    = qDict.get("CEName", "")
            params = qDict.setdefault("ParametersDict", {})

            # ----------------------------
            # 1) Fetch PUE + CI (read-only)
            # ----------------------------
            pue, ci = self._get_site_parameters(site, ce)

            # Store into dict
            params["PUE"] = float(pue)
            params["CI"]  = float(ci)

            # ----------------------------
            # 2) Fetch CE-level CEE (if exists)
            # ----------------------------
            cee = self._get_cee_from_cs(site, ce)
            if cee is None:
                cee = DEFAULT_CEE

            params["CEE"] = float(cee)

            # ----------------------------
            # 3) New Green Score
            # green score = perf(computation)/watt, higher is better/greener
            # ----------------------------
            try:
                green = float(cee) / (float(ci) * float(pue))
            except Exception:
                green = 0.0

            params["GreenScore"] = green

        # ---------------------------------------------------------
        # Sort queues by GREEN_SCORE (descending = greener first)
        # ---------------------------------------------------------
        sorted_items = sorted(
            self.queueDict.items(),
            key=lambda item: item[1]["ParametersDict"].get("GreenScore", 0.0),
            reverse=True
        )
        self.queueDict = dict(sorted_items)

        self._debugTopQueues()
        return S_OK()


    # =================================================================
    # PUE/CI RETRIEVAL
    # =================================================================
    def _get_site_parameters(self, site, ceName):
        """
        Priority:
          1. CIM API
          2. CE-level CS
          3. Defaults
        """

        if not site:
            return DEFAULT_PUE, DEFAULT_CI

        # cache
        key = (site, ceName)
        if key in self.green_cache:
            ts, pue, ci = self.green_cache[key]
            if time.time() - ts < GREEN_CACHE_TTL:
                return pue, ci

        goc_site = self._resolve_gocdb_name(site)

        # --- CIM/CI API ---
        try:
            pue, ci = self._query_cim_ci(goc_site)
            self.green_cache[key] = (time.time(), pue, ci)
            return pue, ci
        except Exception:
            pass

        # --- CE-level CS fallback ---
        pue, ci = self._get_pue_ci_from_cs(site, ceName)
        if pue is not None or ci is not None:
            if pue is None: pue = DEFAULT_PUE
            if ci is None: ci = DEFAULT_CI
            self.green_cache[key] = (time.time(), pue, ci)
            return pue, ci

        # --- Defaults ---
        pue, ci = DEFAULT_PUE, DEFAULT_CI
        self.green_cache[key] = (time.time(), pue, ci)
        return pue, ci


    # =================================================================
    # CE-LEVEL CEE (READ-ONLY)
    # =================================================================
    def _get_cee_from_cs(self, site, ceName):
        """
        Reads (only) from CS:
           /Resources/Sites/<Grid>/<Site>/CEs/<CE>/AverageCEE
        """
        grid = self._find_grid_for_site(site)
        if not grid:
            return None

        base = f"/Resources/Sites/{grid}/{site}/CEs/{ceName}"
        cee_str = gConfig.getValue(f"{base}/AverageCEE", None)

        try:
            return float(cee_str) if cee_str is not None else None
        except Exception:
            return None


    # =================================================================
    # GOCDB name
    # =================================================================
    def _resolve_gocdb_name(self, site):
        try:
            res = getDIRACGOCDictionary()
            if res["OK"]:
                return res["Value"].get(site, site)
        except Exception:
            pass
        return site


    # =================================================================
    # CIM API (PUE + CI)
    # =================================================================
    def _get_jwt_token(self):
        if self._token and self._token_ts:
            age_h = (time.time() - self._token_ts)/3600
            if age_h < TOKEN_MAX_AGE_H:
                return self._token

        if not self.cim_api_base:
            return None

        try:
            url = f"{self.cim_api_base.rstrip('/')}/get-token"
            r = requests.post(url, json={
                "email": self.cim_email, "password": self.cim_password
            }, timeout=10)

            if r.status_code != 200:
                return None

            self._token = r.json().get("access_token")
            self._token_ts = time.time()
            return self._token

        except Exception:
            return None

    def _query_cim_ci(self, goc_site):
        """
        Returns:
            (PUE, CI)
        """
        headers = {"Content-Type": "application/json"}
        tok = self._get_jwt_token()
        if tok:
            headers["Authorization"] = f"Bearer {tok}"

        # ---- GET PUE ----
        pue_url = f"{self.ci_api_base.rstrip('/')}/pue"
        r = requests.post(pue_url, json={"site_name": goc_site},
                          headers=headers, timeout=10)
        if r.status_code != 200:
            raise RuntimeError("PUE query failed")

        data = r.json()
        pue = float(data.get("pue", DEFAULT_PUE))

        loc = data.get("location", {})
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        if not lat or not lon:
            raise RuntimeError("Missing geo coordinates")

        # ---- GET CI ----
        t = datetime.now(timezone.utc) - timedelta(hours=24)
        t_iso = t.isoformat(timespec="seconds").replace("+00:00", "Z")

        payload = {
            "lat": lat,
            "lon": lon,
            "pue": pue,
            "energy_wh": ENERGY_WH_DEFAULT,
            "time": t_iso,
            "metric_id": goc_site,
            "wattnet_params": {"granularity": "hour"},
        }

        ci_url = f"{self.ci_api_base.rstrip('/')}/ci"
        r2 = requests.post(ci_url, json=payload,
                           headers=headers, timeout=10)
        if r2.status_code != 200:
            raise RuntimeError("CI query failed")

        ci_raw = r2.json().get("effective_ci_gco2_per_kwh") \
               or r2.json().get("ci_gco2_per_kwh")

        if ci_raw is None:
            raise RuntimeError("CI missing")

        return pue, float(ci_raw)


    # =================================================================
    # CE-level CS fallback
    # =================================================================
    def _get_pue_ci_from_cs(self, site, ceName):
        grid = self._find_grid_for_site(site)
        if not grid:
            return None, None

        base = f"/Resources/Sites/{grid}/{site}/CEs/{ceName}"

        pue = gConfig.getValue(f"{base}/PUE", None)
        ci  = gConfig.getValue(f"{base}/CI", None)

        try:  pue = float(pue) if pue is not None else None
        except: pue = None

        try:  ci  = float(ci)  if ci  is not None else None
        except: ci = None

        return pue, ci


    # =================================================================
    # Utility: find Grid name
    # =================================================================
    def _find_grid_for_site(self, site):
        res = gConfig.getSections("/Resources/Sites")
        if not res["OK"]:
            return None

        for grid in res["Value"]:
            path = f"/Resources/Sites/{grid}/{site}"
            if gConfig.getSections(path)["OK"]:
                return grid

        return None


    # =================================================================
    # DEBUG HELPERS
    # =================================================================
    def _debugFirstQueues(self):
        self.log.always("🔎 FIRST 10 queueDict entries (input):")
        for i, (qName, qDict) in enumerate(self.queueDict.items()):
            if i >= 10: break
            self.log.always(f"[{i+1}] {qName}: {qDict}")

    def _debugTopQueues(self):
        self.log.always("♻️ TOP 5 GREENEST QUEUES (sorted by CEE/(CI·PUE)):")
        for i, (qName, qDict) in enumerate(list(self.queueDict.items())[:5], 1):
            p = qDict.get("ParametersDict", {})
            self.log.always(
                f"{i}. {qName:<40} "
                f"PUE={p.get('PUE')}, CI={p.get('CI')}, "
                f"CEE={p.get('CEE')}, GREEN_SCORE={p.get('GreenScore')}"
            )
