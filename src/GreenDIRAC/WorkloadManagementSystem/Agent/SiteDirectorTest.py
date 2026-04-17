#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test hook agent for GreenSiteDirector.

This class is self-contained and does not inherit from the production
Green SiteDirector implementation.
"""

from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import (
    SiteDirector as BaseSiteDirector,
)
from GreenDIRAC.WorkloadManagementSystem.Client.CIMClient import CIMClient

import time

CEE_CACHE_TTL = 180  # seconds
DEFAULT_CEE = 1.0
ES_TIME_WINDOW = "now-30d"


class SiteDirector(BaseSiteDirector):
    """Complete SiteDirector test variant with startup diagnostics."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.always("GreenSiteDirectorTest loaded (diagnostic mode)")
        self.cimClient = CIMClient(logger=self.log)
        self._ceeCache = {}
        self._ceeCacheTimestamp = 0
        self.elasticJobParametersDB = None
        self._startupDiagnosticDone = False
        self._lastGreenMetrics = {}

    def _getElasticJobParametersDB(self):
        if self.elasticJobParametersDB:
            return self.elasticJobParametersDB

        res = ObjectLoader().loadObject(
            "WorkloadManagementSystem.DB.ElasticJobParametersDB",
            "ElasticJobParametersDB",
        )
        if not res["OK"]:
            self.log.error(f"Cannot load ElasticJobParametersDB: {res['Message']}")
            return None

        try:
            self.elasticJobParametersDB = res["Value"](parentLogger=self.log)
        except Exception as exc:
            self.log.error(f"Cannot initialize ElasticJobParametersDB: {exc}")
            return None

        return self.elasticJobParametersDB

    def _getSortedQueues(self):
        self.log.always(
            f"GreenSiteDirectorTest: sorting {len(self.queueDict)} queues by GreenScore"
        )
        try:
            greenMetrics = self._computeGreenMetrics()
        except Exception as exc:
            self.log.error(
                f"GreenSiteDirectorTest: failed to compute green metrics, fallback to default order: {exc}"
            )
            return super()._getSortedQueues()

        sortedQueues = sorted(
            self.queueDict.items(),
            key=lambda item: greenMetrics.get(item[0], {}).get("GreenScore", 0.0),
            reverse=True,
        )
        self._lastGreenMetrics = greenMetrics

        self.log.always("GreenSiteDirectorTest: TOP 5 QUEUES AFTER GREEN SORT")
        for i, (qName, _qDict) in enumerate(sortedQueues[:5], 1):
            gm = greenMetrics.get(qName, {})
            self.log.always(
                f"  {i}. {qName:<45} "
                f"GREEN={gm.get('GreenScore', 0.0):.4f} "
                f"PUE={gm.get('PUE')} "
                f"CI={gm.get('CI')} "
                f"CEE={gm.get('CEE')}"
            )
        return sortedQueues

    def _computeGreenMetrics(self):
        if not self.queueDict:
            return {}

        avgCEE = self._getAverageCEEFromES()
        greenMetrics = {}

        # Resolve PUE/CI once per unique site to avoid repeated calls for queues sharing a site.
        siteMetrics = {}
        uniqueSites = {qDict.get("Site", "") for qDict in self.queueDict.values()}
        for site in uniqueSites:
            try:
                pue, ci, _gocdb = self.cimClient.getSiteGreenMetrics(site)
            except Exception:
                pue, ci = 3.0, 1000.0
            siteMetrics[site] = (pue, ci)

        for qName, qDict in self.queueDict.items():
            site = qDict.get("Site", "")
            ce = qDict.get("CEName", "")
            pue, ci = siteMetrics.get(site, (3.0, 1000.0))

            cee = avgCEE.get(ce, DEFAULT_CEE)
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

    def _getAverageCEEFromES(self):
        now = time.time()
        if self._ceeCache and (now - self._ceeCacheTimestamp) < CEE_CACHE_TTL:
            self.log.debug(
                "GreenSiteDirectorTest: using cached CEE values "
                f"(age={int(now - self._ceeCacheTimestamp)}s)"
            )
            return self._ceeCache

        self.log.info("GreenSiteDirectorTest: refreshing CEE cache from Elasticsearch")
        db = self._getElasticJobParametersDB()
        if not db:
            return self._ceeCache

        indexPattern = f"{db.indexName_base}_*"
        self.log.info(
            "GreenSiteDirectorTest: querying ES via ElasticJobParametersDB "
            f"(host={getattr(db, '_dbHost', 'n/a')}, port={getattr(db, '_dbPort', 'n/a')}, "
            f"indexPattern={indexPattern}, window={ES_TIME_WINDOW})"
        )

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

        res = db.query(index=indexPattern, query=query)
        if not res["OK"]:
            self.log.error(f"GreenSiteDirectorTest: ES aggregation failed: {res['Message']}")
            return self._ceeCache

        buckets = (
            res["Value"].get("aggregations", {})
            .get("by_gridce", {})
            .get("buckets", [])
        )
        self.log.info(
            "GreenSiteDirectorTest: ES aggregation response "
            f"(buckets={len(buckets)})"
        )

        avgCEE = {}
        for bucket in buckets:
            if bucket.get("key") and bucket.get("avg_cee", {}).get("value") is not None:
                try:
                    avgCEE[bucket["key"]] = float(bucket["avg_cee"]["value"])
                except Exception:
                    pass

        self._ceeCache = avgCEE
        self._ceeCacheTimestamp = now
        self.log.info(
            f"GreenSiteDirectorTest: cached CEE for {len(avgCEE)} GridCEs "
            f"(indexPattern={indexPattern})"
        )
        for ceName in sorted(avgCEE):
            self.log.info(
                f"GreenSiteDirectorTest: AVG_CEE CE={ceName} value={avgCEE[ceName]:.6f}"
            )
        return avgCEE

    def execute(self):
        if not self._startupDiagnosticDone:
            self._runStartupDiagnostics()
            self._startupDiagnosticDone = True
        return super().execute()

    def _runStartupDiagnostics(self):
        self.log.info("SiteDirectorTest: starting one-time startup diagnostics")

        try:
            avgCEE = self._getAverageCEEFromES()
            self.log.info(
                "GreenSiteDirectorTest: ES diagnostic OK "
                f"(cachedGridCEs={len(avgCEE) if isinstance(avgCEE, dict) else 'n/a'})"
            )
        except Exception as exc:
            self.log.error(f"GreenSiteDirectorTest: ES diagnostic failed: {exc}")

        try:
            sortedQueues = self._getSortedQueues()
            greenMetrics = self._lastGreenMetrics or {}
            scores = [greenMetrics.get(qName, {}).get("GreenScore", 0.0) for qName, _ in sortedQueues]
            isDescending = all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1))
            self.log.info(
                "GreenSiteDirectorTest: sorting diagnostic "
                f"(queues={len(sortedQueues)}, descending={isDescending})"
            )
            for i, (qName, qDict) in enumerate(sortedQueues[:5], 1):
                gm = greenMetrics.get(qName, {})
                self.log.info(
                    f"GreenSiteDirectorTest: TOP{i:02d} queue={qName} "
                    f"CE={qDict.get('CEName', '')} "
                    f"GreenScore={gm.get('GreenScore', 0.0):.8f} "
                    f"CEE={gm.get('CEE', 0.0):.6f}"
                )
        except Exception as exc:
            self.log.error(f"GreenSiteDirectorTest: sorting diagnostic failed: {exc}")


class SiteDirectorTest(SiteDirector):
    """Alias class in case module loaders expect SiteDirectorTest symbol."""
