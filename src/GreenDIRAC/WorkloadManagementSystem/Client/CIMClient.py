"""
CIMClient — Unified client for GreenDIGIT CIM + KPI services.

Responsibilities:
- Read configuration from cim.conf
- Authenticate and cache JWT token (gd-cim-api)
- Query PUE and CI (gd-kpi-api, Authorization required)
- Submit green job records to CIM (gd-cim-api)
"""

import os
import time
import configparser
import requests
from datetime import datetime, timezone, timedelta

from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary


# ==========================================================
# Defaults
# ==========================================================
DEFAULT_PUE = 3
DEFAULT_CI = 1000
DEFAULT_ENERGY_WH = 8500

DEFAULT_TOKEN_MAX_AGE_H = 24
DEFAULT_CACHE_TTL = 300


# ==========================================================
# CIMClient
# ==========================================================
class CIMClient:

    def __init__(self, confFile=None, logger=None):
        self.log = logger
        self._loadConfig(confFile)

        # JWT cache
        self._token = None
        self._token_ts = None

        # site cache: site -> (timestamp, pue, ci, gocdb)
        self._site_cache = {}

    # ======================================================
    # CONFIGURATION
    # ======================================================
    def _loadConfig(self, confFile):

        if not confFile:
            confFile = os.path.join(os.path.dirname(__file__), "cim.conf")

        if not os.path.exists(confFile):
            raise RuntimeError(f"CIMClient config file not found: {confFile}")

        cfg = configparser.ConfigParser()
        cfg.read(confFile)

        # --- CIM (authenticated) ---
        self.cim_email = cfg.get("CIM", "EMAIL")
        self.cim_password = cfg.get("CIM", "PASSWORD")
        self.cim_api_base = cfg.get("CIM", "API_BASE").rstrip("/")
        self.metrics_url = cfg.get("CIM", "METRICS_URL").rstrip("/")

        # --- KPI ---
        self.kpi_api_base = cfg.get("KPI", "API_BASE").rstrip("/")

        # --- Defaults ---
        self.default_pue = cfg.getfloat("Defaults", "PUE", fallback=DEFAULT_PUE)
        self.default_ci = cfg.getfloat("Defaults", "CI", fallback=DEFAULT_CI)
        self.default_energy_wh = cfg.getint(
            "Defaults", "ENERGY_WH", fallback=DEFAULT_ENERGY_WH
        )

        # --- Runtime ---
        self.token_max_age_h = cfg.getfloat(
            "Runtime", "TOKEN_MAX_AGE_H", fallback=DEFAULT_TOKEN_MAX_AGE_H
        )
        self.cache_ttl = cfg.getint(
            "Runtime", "CACHE_TTL", fallback=DEFAULT_CACHE_TTL
        )

    # ======================================================
    # TOKEN (gd-cim-api)
    # ======================================================
    def _getToken(self):

        if self._token and self._token_ts:
            age_h = (time.time() - self._token_ts) / 3600.0
            if age_h < self.token_max_age_h:
                return self._token

        url = f"{self.cim_api_base}/token"

        r = requests.get(
            url,
            params={
                "email": self.cim_email,
                "password": self.cim_password,
            },
            timeout=10,
        )
        r.raise_for_status()

        data = r.json()
        self._token = data.get("access_token")
        self._token_ts = time.time()

        if not self._token:
            raise RuntimeError("CIM authentication failed (no token returned)")

        return self._token

    # ======================================================
    # PUBLIC: SITE GREEN METRICS
    # ======================================================
    def getSiteGreenMetrics(self, site, ceName=None):
        """
        Returns:
            (PUE, CI, GOCDB_SITE)
        """

        now = time.time()
        if site in self._site_cache:
            ts, pue, ci, gocdb = self._site_cache[site]
            if now - ts < self.cache_ttl:
                return pue, ci, gocdb

        gocdb = self._resolveGOCDB(site)

        try:
            pue, ci = self._queryPUEandCI(gocdb)
        except Exception as e:
            if self.log:
                self.log.error(
                    f"CIMClient failure for site={site} gocdb={gocdb}: {e}"
                )
            pue, ci = self.default_pue, self.default_ci

        self._site_cache[site] = (now, pue, ci, gocdb)
        return pue, ci, gocdb

    def _resolveGOCDB(self, site):
        try:
            res = getDIRACGOCDictionary()
            if res["OK"]:
                return res["Value"].get(site, site)
        except Exception:
            pass
        return site

    # ======================================================
    # KPI API: PUE + CI
    # ======================================================
    def _queryPUEandCI(self, gocdb):

        headers = {
            "Authorization": f"Bearer {self._getToken()}",
            "Content-Type": "application/json",
        }

        # ---- PUE ----
        pue_resp = requests.post(
            f"{self.kpi_api_base}/pue",
            json={"site_name": gocdb},
            headers=headers,
            timeout=10,
        )
        pue_resp.raise_for_status()

        pue_data = pue_resp.json()
        pue = float(pue_data.get("pue", self.default_pue))

        loc = pue_data.get("location", {})
        lat = loc.get("latitude")
        lon = loc.get("longitude")

        if not lat or not lon:
            if self.log:
                self.log.warn(
                    f"No coordinates for site={gocdb}, using default CI"
                )
            return pue, self.default_ci

        # ---- CI (1h window) ----
        now = datetime.now(timezone.utc)
        end = now.isoformat(timespec="seconds").replace("+00:00", "Z")
        start = (now - timedelta(hours=1)).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")

        payload = {
            "lat": lat,
            "lon": lon,
            "pue": pue,
            "energy_wh": self.default_energy_wh,
            "start": start,
            "end": end,
            "metric_id": gocdb,
            "wattnet_params": {"granularity": "hour"},
        }

        ci_resp = requests.post(
            f"{self.kpi_api_base}/ci",
            json=payload,
            headers=headers,
            timeout=10,
        )

        ci = self.default_ci

        if ci_resp.status_code == 200:
            try:
                ci_data = ci_resp.json()
                ci = (
                   #ci_data.get("effective_ci_gco2_per_kwh")
                     ci_data.get("ci_gco2_per_kwh")
                    or self.default_ci
                )
            except Exception as e:
                if self.log:
                    self.log.warn(
                        f"CI parse error for site={gocdb}: {e}"
                    )
        else:
            if self.log:
                self.log.warn(
                    f"CI unavailable for site={gocdb}: {ci_resp.text}"
                )

        return pue, float(ci)

    # ======================================================
    # WRITE: SUBMIT RECORD TO CIM
    # ======================================================
    def submitRecord(self, record):
        """
        Submit a green job record to CIM.

        Returns:
            True on success, False on failure
        """

        headers = {
            "Authorization": f"Bearer {self._getToken()}",
            "Content-Type": "application/json",
        }

        resp = requests.post(
            self.metrics_url,
            json=record,
            headers=headers,
            timeout=15,
        )

        if resp.status_code not in (200, 201):
            if self.log:
                self.log.error(
                    f"CIM submission failed "
                    f"[{resp.status_code}]: {resp.text}"
                )
            return False

        return True
