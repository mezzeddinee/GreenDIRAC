#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GreenCEMetricsAgent — periodically queries CIM CI API for each DIRAC CE
and writes PUE/CI values into the DIRAC Configuration System.

This agent:
  ✔ Scans all Sites under /Resources/Sites
  ✔ For every CE inside every Site:
       - Resolve GOCDB name
       - Query CIM/CI for PUE + CI
       - If CIM fails → skip (or fallback to defaults)
  ✔ Writes PUE and CI directly to:
       /Resources/Sites/<Grid>/<Site>/CEs/<CEName>/PUE
       /Resources/Sites/<Grid>/<Site>/CEs/<CEName>/CI
  ✔ Commits the CS if changes were made
"""

import time
import requests
from datetime import datetime, timezone, timedelta

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

DEFAULT_PUE = 5.0
DEFAULT_CI = 9999.0
ENERGY_WH_DEFAULT = 8500
TOKEN_MAX_AGE_H = 24.0


class CIM2CSAgent(AgentModule):
    """Simple agent that updates CE-level PUE and CI values."""

    def __init__(self, *args, **kwargs):
        super(CIM2CSAgent, self).__init__(*args, **kwargs)

        self.cim_api_base = self.am_getOption("CIM_API_BASE", "").strip()
        self.ci_api_base = self.am_getOption("CI_API_BASE", "").strip()
        self.cim_email = self.am_getOption("CIM_EMAIL", "").strip()
        self.cim_password = self.am_getOption("CIM_PASSWORD", "").strip()

        self._token = None
        self._token_ts = None

        self.csAPI = None

    # ------------------------------------------------------------------
    def initialize(self):
        """Initialize the agent."""
        return S_OK()

    # ------------------------------------------------------------------
    def execute(self):
        """Main cycle: scan all GRIDs → Sites → CEs and sync PUE/CI."""

        self.log.info("🌿 GreenCEMetricsAgent cycle starting")

        res = gConfig.getSections("/Resources/Sites")
        if not res["OK"]:
            return S_ERROR("Cannot list /Resources/Sites")

        gridList = res["Value"]
        self._ensure_csAPI()
        self._csDirty = False

        for grid in gridList:
            self._process_grid(grid)

        if self._csDirty:
            self.log.info("Committing updated PUE/CI to CS...")
            commit = self.csAPI.commit()
            if not commit["OK"]:
                self.log.error(f"❌ CS commit failed: {commit['Message']}")
            else:
                self.log.info("✅ CS commit successful")

        return S_OK()

    # ------------------------------------------------------------------
    def _process_grid(self, grid):
        """Process all DIRAC sites under a given grid."""

        res = gConfig.getSections(f"/Resources/Sites/{grid}")
        if not res["OK"]:
            return

        for site in res["Value"]:

            # ------------------------------------------------------
            # NEW LOGIC: SKIP sites whose name does NOT start with EGI
            # those are CLOUD TEST==> no PUE, CI...
            # ------------------------------------------------------
            if not site.startswith("EGI"):
                self.log.info(f"Skipping non-EGI site: {site}")
                continue

            self._process_site(grid, site)

    # ------------------------------------------------------------------
    def _process_site(self, grid, site):
        """Process all CEs under a DIRAC Site."""

        path = f"/Resources/Sites/{grid}/{site}/CEs"
        res = gConfig.getSections(path)
        if not res["OK"]:
            return

        ceList = res["Value"]
        gocdb = self._resolve_gocdb_name(site)

        for ce in ceList:
            self._process_ce(grid, site, ce, gocdb)

    # ------------------------------------------------------------------
    def _process_ce(self, grid, site, ceName, gocdbName):
        """Query CIM for a CE's site-level PUE/CI, then write to CS."""

        try:
            pue, ci = self._query_cim_ci(gocdbName)
        except Exception as e:
            self.log.warn(f"[CIM] Failed for {site}/{ceName}: {e}")
            return

        ce_path = f"/Resources/Sites/{grid}/{site}/CEs/{ceName}"

        oldPUE = gConfig.getValue(f"{ce_path}/PUE", None)
        oldCI = gConfig.getValue(f"{ce_path}/CI", None)

        if str(oldPUE) == f"{pue:.3f}" and str(oldCI) == f"{ci:.1f}":
            return  # Already up-to-date

        self.log.info(f"Updating CE {site}/{ceName}: PUE={pue:.3f}, CI={ci:.1f}")

        self.csAPI.setOption(f"{ce_path}/PUE", f"{pue:.3f}")
        self.csAPI.setOption(f"{ce_path}/CI", f"{ci:.1f}")
        self._csDirty = True

    # ------------------------------------------------------------------
    def _resolve_gocdb_name(self, diracSite):
        """DIRAC → GOCDB name mapping."""
        try:
            res = getDIRACGOCDictionary()
            if res["OK"]:
                return res["Value"].get(diracSite, diracSite)
        except Exception:
            pass
        return diracSite

    # ------------------------------------------------------------------
    def _get_jwt_token(self):
        """Request / refresh CIM JWT token."""

        if self._token and self._token_ts:
            age_h = (time.time() - self._token_ts) / 3600.0
            if age_h < TOKEN_MAX_AGE_H:
                return self._token

        if not self.cim_api_base or not self.cim_email or not self.cim_password:
            return None

        try:
            url = f"{self.cim_api_base.rstrip('/')}/get-token"
            r = requests.post(url, json={"email": self.cim_email, "password": self.cim_password}, timeout=10)
            if r.status_code != 200:
                return None
            tok = r.json().get("access_token")
            self._token = tok
            self._token_ts = time.time()
            return tok
        except Exception:
            return None

    # ------------------------------------------------------------------
    def _query_cim_ci(self, gocdbName):
        """Call /pue and /ci from CI_API_BASE."""

        if not self.ci_api_base:
            raise RuntimeError("CI_API_BASE missing")

        headers = {"Content-Type": "application/json"}
        token = self._get_jwt_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # ---- PUE ----
        r = requests.post(
            f"{self.ci_api_base.rstrip('/')}/pue",
            headers=headers,
            json={"site_name": gocdbName},
            timeout=10,
        )
        if r.status_code != 200:
            raise RuntimeError("PUE query failed")

        data = r.json()
        pue = float(data.get("pue", DEFAULT_PUE))

        lat = data.get("location", {}).get("latitude")
        lon = data.get("location", {}).get("longitude")
        if lat is None or lon is None:
            raise RuntimeError("Missing coordinates")

        # ---- CI ----
        offset_h = 5
        t_iso = (datetime.now(timezone.utc) - timedelta(hours=offset_h)) \
            .isoformat(timespec="seconds").replace("+00:00", "Z")

        ci_payload = {
            "lat": lat,
            "lon": lon,
            "pue": pue,
            "energy_wh": ENERGY_WH_DEFAULT,
            "time": t_iso,
            "metric_id": gocdbName,
            "wattnet_params": {"granularity": "hour"},
        }

        r2 = requests.post(
            f"{self.ci_api_base.rstrip('/')}/ci",
            headers=headers,
            json=ci_payload,
            timeout=10,
        )
        if r2.status_code != 200:
            raise RuntimeError("CI query failed")

        ci_raw = r2.json().get("ci_gco2_per_kwh")
        effective = r2.json().get("effective_ci_gco2_per_kwh")

        ci = effective if effective is not None else (ci_raw * pue if ci_raw else DEFAULT_CI)

        return pue, float(ci)

    # ------------------------------------------------------------------
    def _ensure_csAPI(self):
        if self.csAPI is None:
            self.csAPI = CSAPI()
