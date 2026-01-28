#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GreenSiteDirector — READ-ONLY green-aware SiteDirector

- Does NOT override beginExecution()
- Overrides _getSortedQueues()
- Adds explicit logs showing queue ordering
"""

from __future__ import annotations

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import (
    SiteDirector as BaseSiteDirector,
)

from GreenDIRAC.WorkloadManagementSystem.Client.CIMClient import CIMClient
from elasticsearch import Elasticsearch

import time

CEE_CACHE_TTL = 180  # seconds (10 minutes)


# =====================================================================
# CONSTANTS
# =====================================================================
DEFAULT_CEE = 1.0

ES_HOST = "https://elias-beta.cc.in2p3.fr:9200"
ES_INDEX_PATTERN = "egi-fg-dirac-_elasticjobparameters_index_*"
ES_TIME_WINDOW = "now-30d"

CERT_DIR = "/vo/dirac/etc/grid-security/ES"
ES_CA_CERT = f"{CERT_DIR}/ca.crt"
ES_CLIENT_CERT = f"{CERT_DIR}/egirobot.crt"
ES_CLIENT_KEY = f"{CERT_DIR}/egirobot.key"


# =====================================================================
# GREEN SITE DIRECTOR
# =====================================================================
class SiteDirector(BaseSiteDirector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.always("GreenSiteDirector loaded (GreenScore-based queue ordering)")
        self.cimClient = CIMClient()

        # ---- CEE CACHE ----
        self._ceeCache = {}
        self._ceeCacheTimestamp = 0

    # =================================================================
    # QUEUE SORTING OVERRIDE (WITH LOGS)
    # =================================================================
    def _getSortedQueues(self):
        """
        Sort queues by GreenScore (descending) and log top 5.
        """

        self.log.always(
            f"GreenSiteDirector: sorting {len(self.queueDict)} queues by GreenScore"
        )

        try:
            greenMetrics = self._computeGreenMetrics()
        except Exception as e:
            self.log.error(
                f"GreenSiteDirector: failed to compute green metrics, "
                f"falling back to random order: {e}"
            )
            return super()._getSortedQueues()

        # sortedQueues = sorted(
        #     self.queueDict.items(),
        #     key=lambda item: item[1]
        #     .get("ParametersDict", {})
        #     .get("GreenScore", 0.0),
        #     reverse=True,
        # )

        sortedQueues = sorted(
            self.queueDict.items(),
            key=lambda item: greenMetrics
            .get(item[0], {})
            .get("GreenScore", 0.0),
            reverse=True,
        )

        # ---- LOG TOP 5 SORTED QUEUES ----
        self.log.always("GreenSiteDirector: TOP 5 QUEUES AFTER GREEN SORT")
        for i, (qName, qDict) in enumerate(sortedQueues[:5], 1):
            gm = greenMetrics.get(qName, {})
            self.log.always(
                f"  {i}. {qName:<45} "
                f"GREEN={gm.get('GreenScore', 0.0):.4f} "
                f"PUE={gm.get('PUE')} "
                f"CI={gm.get('CI')} "
                f"CEE={gm.get('CEE')}"
            )
        return sortedQueues

    # =================================================================
    # GREEN METRICS COMPUTATION
    # =================================================================
    def _computeGreenMetrics(self):
        if not self.queueDict:
            return {}

        avgCEE = self._getAverageCEEFromES()
        greenMetrics = {}

        for qName, qDict in self.queueDict.items():
            site = qDict.get("Site", "")
            ce = qDict.get("CEName", "")

            # PUE / CI
            try:
                pue, ci, gocdb = self.cimClient.getSiteGreenMetrics(site)
            except Exception:
                pue, ci = 3.0, 1000.0

            # CEE
            cee = avgCEE.get(ce, DEFAULT_CEE)

            # GreenScore
            try:
                greenScore = float(cee) / (float(ci) * float(pue))
            except Exception:
                greenScore = 0.0

            greenMetrics[qName] = {
                "PUE": float(pue),
                "CI": float(ci),
                "CEE": float(cee),
                "GreenScore": greenScore,
            }

        return greenMetrics
    # =================================================================
    # ELASTICSEARCH: AVERAGE CEE
    # =================================================================
    def _getAverageCEEFromES(self):
        now = time.time()

        # ---- CACHE HIT ----
        if (
                self._ceeCache
                and (now - self._ceeCacheTimestamp) < CEE_CACHE_TTL
        ):
            self.log.debug(
                "GreenSiteDirector: using cached CEE values "
                f"(age={int(now - self._ceeCacheTimestamp)}s)"
            )
            return self._ceeCache

        self.log.info("GreenSiteDirector: refreshing CEE cache from Elasticsearch")

        # ---- ES CLIENT ----
        try:
            es = Elasticsearch(
                [ES_HOST],
                scheme="https",
                port=9200,
                verify_certs=True,
                ca_certs=ES_CA_CERT,
                client_cert=ES_CLIENT_CERT,
                client_key=ES_CLIENT_KEY,
                ssl_show_warn=False,
            )
        except Exception as e:
            self.log.error(f"GreenSiteDirector: failed to create ES client: {e}")
            return self._ceeCache  # fallback to last known values

        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": ES_TIME_WINDOW}}},
                        {"exists": {"field": "CEE"}},
                        {"exists": {"field": "GridCE"}},
                    ]
                }
            },
            "aggs": {
                "by_gridce": {
                    "terms": {"field": "GridCE", "size": 10000},
                    "aggs": {"avg_cee": {"avg": {"field": "CEE"}}},
                }
            },
        }

        try:
            res = es.search(index=ES_INDEX_PATTERN, body=query)
        except Exception as e:
            self.log.error(f"GreenSiteDirector: ES aggregation failed: {e}")
            return self._ceeCache  # fallback

        buckets = (
            res.get("aggregations", {})
            .get("by_gridce", {})
            .get("buckets", [])
        )

        avgCEE = {}
        for b in buckets:
            if b.get("key") and b.get("avg_cee", {}).get("value") is not None:
                try:
                    avgCEE[b["key"]] = float(b["avg_cee"]["value"])
                except Exception:
                    pass

        # ---- UPDATE CACHE ----
        self._ceeCache = avgCEE
        self._ceeCacheTimestamp = now

        self.log.info(
            f"GreenSiteDirector: cached CEE for {len(avgCEE)} GridCEs"
        )

        return avgCEE