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
DEFAULT_TOKEN_TIMEOUT_S = 20
DEFAULT_PUE_TIMEOUT_S = 20
DEFAULT_CI_TIMEOUT_S = 30
DEFAULT_SUBMIT_TIMEOUT_S = 30


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
        self.token_timeout_s = cfg.getfloat(
            "Runtime", "TOKEN_TIMEOUT_S", fallback=DEFAULT_TOKEN_TIMEOUT_S
        )
        self.pue_timeout_s = cfg.getfloat(
            "Runtime", "PUE_TIMEOUT_S", fallback=DEFAULT_PUE_TIMEOUT_S
        )
        self.ci_timeout_s = cfg.getfloat(
            "Runtime", "CI_TIMEOUT_S", fallback=DEFAULT_CI_TIMEOUT_S
        )
        self.submit_timeout_s = cfg.getfloat(
            "Runtime", "SUBMIT_TIMEOUT_S", fallback=DEFAULT_SUBMIT_TIMEOUT_S
        )

    # ======================================================
    # TOKEN (gd-cim-api)
    # ======================================================
    def _getToken(self):
        def _safe_preview(value, limit=1000):
            text = str(value)
            if len(text) > limit:
                return f"{text[:limit]}...(truncated {len(text) - limit} chars)"
            return text

        if self._token and self._token_ts:
            age_h = (time.time() - self._token_ts) / 3600.0
            if age_h < self.token_max_age_h:
                if self.log:
                    self.log.info(
                        f"[CIMClient::_getToken] Reusing cached token age_h={age_h:.3f}"
                    )
                return self._token

        url = f"{self.cim_api_base}/token"
        auth_payload = {"email": self.cim_email, "password": self.cim_password}
        auth_attempts = (
            ("POST", {"json": auth_payload}),
            ("POST", {"data": auth_payload}),
            ("GET", {"params": auth_payload}),
        )
        last_error = None

        for method, kwargs in auth_attempts:
            try:
                if self.log:
                    body_kind = "params" if "params" in kwargs else ("json" if "json" in kwargs else "form-data")
                    self.log.info(
                        f"[CIMClient::_getToken] Trying token request method={method} "
                        f"url={url} body_kind={body_kind} email={self.cim_email} "
                        f"timeout_s={self.token_timeout_s}"
                    )

                t0 = time.time()
                resp = requests.request(method, url, timeout=self.token_timeout_s, **kwargs)
                dt = time.time() - t0

                if self.log:
                    self.log.info(
                        f"[CIMClient::_getToken] Token response method={method} "
                        f"status={resp.status_code} elapsed_s={dt:.3f} "
                        f"body={_safe_preview(resp.text)}"
                    )

                if resp.status_code >= 400:
                    last_error = RuntimeError(
                        f"Token request failed with status={resp.status_code} "
                        f"method={method}"
                    )
                    continue

                data = resp.json()
                token = data.get("access_token")
                if not token:
                    last_error = RuntimeError(
                        f"No access_token in token response (method={method})"
                    )
                    if self.log:
                        self.log.warn(
                            f"[CIMClient::_getToken] Missing access_token in response "
                            f"method={method} json={_safe_preview(data)}"
                        )
                    continue

                self._token = token
                self._token_ts = time.time()
                if self.log:
                    token_preview = (
                        f"{token[:12]}...{token[-8:]}" if len(token) > 20 else "short-token"
                    )
                    self.log.info(
                        f"[CIMClient::_getToken] Token acquired successfully "
                        f"method={method} token_preview={token_preview}"
                    )
                return self._token
            except Exception as e:
                last_error = e
                if self.log:
                    self.log.warn(
                        f"[CIMClient::_getToken] Token attempt failed method={method}: {e}"
                    )

        raise RuntimeError(f"CIM authentication failed after all attempts: {last_error}")

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

        cacheable = True
        try:
            pue, ci, cacheable = self._queryPUEandCI(gocdb)
        except Exception as e:
            if self.log:
                self.log.error(
                    f"CIMClient failure for site={site} gocdb={gocdb}: {e}"
                )
            pue, ci = self.default_pue, self.default_ci
            cacheable = False

        if cacheable:
            self._site_cache[site] = (now, pue, ci, gocdb)
        elif self.log:
            self.log.warn(
                f"[CIMClient::getSiteGreenMetrics] Not caching fallback/degraded "
                f"values for site={site} gocdb={gocdb} pue={pue} ci={ci}"
            )
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
        def _safe_preview(value, limit=2000):
            text = str(value)
            if len(text) > limit:
                return f"{text[:limit]}...(truncated {len(text) - limit} chars)"
            return text

        cacheable = True
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] START gocdb={gocdb} "
                f"kpi_api_base={self.kpi_api_base}"
            )

        token = self._getToken()
        token_preview = f"{token[:12]}...{token[-8:]}" if token and len(token) > 20 else "short-token"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] Auth token acquired token_preview={token_preview}"
            )

        # ---- PUE ----
        pue_url = f"{self.kpi_api_base}/pue"
        pue_payload = {"site_name": gocdb}
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] PUE request url={pue_url} "
                f"payload={pue_payload} timeout_s={self.pue_timeout_s}"
            )

        pue_t0 = time.time()
        pue_resp = requests.post(
            pue_url,
            json=pue_payload,
            headers=headers,
            timeout=self.pue_timeout_s,
        )
        pue_dt = time.time() - pue_t0
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] PUE response status={pue_resp.status_code} "
                f"elapsed_s={pue_dt:.3f} body={_safe_preview(pue_resp.text)}"
            )
        pue_resp.raise_for_status()

        pue_data = pue_resp.json()
        raw_pue = pue_data.get("pue", self.default_pue)
        try:
            pue = float(raw_pue)
        except (TypeError, ValueError):
            cacheable = False
            if self.log:
                self.log.warn(
                    f"[CIMClient::_queryPUEandCI] Invalid PUE value for site={gocdb}: "
                    f"raw_pue={raw_pue}; using default PUE={self.default_pue}"
                )
            pue = float(self.default_pue)
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] PUE parsed pue={pue} "
                f"json={_safe_preview(pue_data)}"
            )

        loc = pue_data.get("location", {})
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] Coordinates extracted "
                f"lat={lat} lon={lon}"
            )

        if not lat or not lon:
            cacheable = False
            if self.log:
                self.log.warn(
                    f"[CIMClient::_queryPUEandCI] No coordinates for site={gocdb}, "
                    f"using default CI={self.default_ci}"
                )
            return pue, self.default_ci, cacheable

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
        ci_url = f"{self.kpi_api_base}/ci"
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] CI request url={ci_url} "
                f"payload={_safe_preview(payload)} timeout_s={self.ci_timeout_s}"
            )

        ci_t0 = time.time()
        ci_resp = requests.post(
            ci_url,
            json=payload,
            headers=headers,
            timeout=self.ci_timeout_s,
        )
        ci_dt = time.time() - ci_t0
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] CI response status={ci_resp.status_code} "
                f"elapsed_s={ci_dt:.3f} body={_safe_preview(ci_resp.text)}"
            )

        ci = self.default_ci
        ci_status_docs = {
            200: "OK - CI value returned",
            400: "Bad Request - invalid/missing payload fields",
            401: "Unauthorized - token missing/expired/invalid",
            403: "Forbidden - caller has no access",
            404: "Not Found - endpoint or metric target unavailable",
            422: "Unprocessable Entity - payload format accepted but semantically invalid",
            429: "Too Many Requests - rate limited",
            500: "Internal Server Error - upstream/server failure",
            502: "Bad Gateway - upstream proxy/backend failure",
            503: "Service Unavailable - temporary outage/maintenance",
            504: "Gateway Timeout - upstream backend timeout",
        }
        ci_status_meaning = ci_status_docs.get(ci_resp.status_code, "Unhandled status code")
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] CI status meaning: "
                f"{ci_resp.status_code} => {ci_status_meaning}"
            )

        # CI response-code handling:
        # 200 -> parse returned value
        # all others -> log documented meaning and keep fallback default CI
        if ci_resp.status_code == 200:
            try:
                ci_data = ci_resp.json()
                raw_ci = (
                    # ci_data.get("effective_ci_gco2_per_kwh")
                    ci_data.get("ci_gco2_per_kwh")
                )
                if raw_ci is None:
                    cacheable = False
                    ci = float(self.default_ci)
                    if self.log:
                        self.log.warn(
                            f"[CIMClient::_queryPUEandCI] Missing CI value for site={gocdb}; "
                            f"using default CI={self.default_ci}"
                        )
                else:
                    try:
                        ci = float(raw_ci)
                    except (TypeError, ValueError):
                        cacheable = False
                        if self.log:
                            self.log.warn(
                                f"[CIMClient::_queryPUEandCI] Invalid CI value for site={gocdb}: "
                                f"raw_ci={raw_ci}; using default CI={self.default_ci}"
                            )
                        ci = float(self.default_ci)
                if self.log:
                    self.log.info(
                        f"[CIMClient::_queryPUEandCI] CI parsed ci={ci} "
                        f"json={_safe_preview(ci_data)}"
                    )
            except Exception as e:
                cacheable = False
                if self.log:
                    self.log.warn(
                        f"[CIMClient::_queryPUEandCI] CI parse error for site={gocdb}: {e}"
                    )
        elif ci_resp.status_code in (400, 422):
            cacheable = False
            if self.log:
                self.log.warn(
                    f"[CIMClient::_queryPUEandCI] CI client payload issue for site={gocdb}: "
                    f"status={ci_resp.status_code} meaning={ci_status_meaning} "
                    f"body={_safe_preview(ci_resp.text)}"
                )
        elif ci_resp.status_code in (401, 403):
            cacheable = False
            if self.log:
                self.log.error(
                    f"[CIMClient::_queryPUEandCI] CI auth/permission issue for site={gocdb}: "
                    f"status={ci_resp.status_code} meaning={ci_status_meaning} "
                    f"body={_safe_preview(ci_resp.text)}"
                )
        elif ci_resp.status_code in (429,):
            cacheable = False
            if self.log:
                self.log.warn(
                    f"[CIMClient::_queryPUEandCI] CI rate-limited for site={gocdb}: "
                    f"status={ci_resp.status_code} meaning={ci_status_meaning} "
                    f"body={_safe_preview(ci_resp.text)}"
                )
        elif ci_resp.status_code in (500, 502, 503, 504):
            cacheable = False
            if self.log:
                self.log.error(
                    f"[CIMClient::_queryPUEandCI] CI server-side failure for site={gocdb}: "
                    f"status={ci_resp.status_code} meaning={ci_status_meaning} "
                    f"body={_safe_preview(ci_resp.text)}"
                )
        else:
            cacheable = False
            if self.log:
                self.log.warn(
                    f"[CIMClient::_queryPUEandCI] CI unavailable for site={gocdb}: "
                    f"status={ci_resp.status_code} meaning={ci_status_meaning} "
                    f"body={_safe_preview(ci_resp.text)}"
                )

        result_pue, result_ci = float(pue), float(ci)
        if self.log:
            self.log.info(
                f"[CIMClient::_queryPUEandCI] END gocdb={gocdb} "
                f"result_pue={result_pue} result_ci={result_ci} cacheable={cacheable}"
            )
        return result_pue, result_ci, cacheable

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
            timeout=self.submit_timeout_s,
        )

        if resp.status_code not in (200, 201):
            if self.log:
                self.log.error(
                    f"CIM submission failed "
                    f"[{resp.status_code}]: {resp.text}"
                )
            return False

        return True
