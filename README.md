# GreenDIRAC

GreenDIRAC is a DIRAC extension built for the GreenDIGIT Project. It enriches
DIRAC workload management with sustainability data—power usage effectiveness
(PUE), carbon intensity (CI) and compute energy efficiency (CEE)—so scheduling
and reporting tools can account for the environmental cost of running jobs.

## What GreenDIRAC adds
- Registers itself as a DIRAC extension so the agents and configuration entries
  are available alongside standard DIRAC deployments.
- Pulls PUE/CI values from external CIM/CI services and writes them into the
  DIRAC Configuration System, so schedulers can reuse the data without making
  direct API calls.
- Computes and stores CEE-derived averages for each Computing Element (CE)
  based on recently finished jobs.
- Uses the collected metrics to prefer greener queues when dispatching jobs and
  to record energy/emissions for completed workloads.

## Core agents
- **Green-aware Site Director** – augments queue information with PUE, CI and
  CEE, then computes a green score that ranks queues from greenest to least
  green before scheduling.
- **CIM2CSAgent** – synchronizes PUE and CI values from CIM/CI APIs into the
  DIRAC Configuration System for each site/CE, including token refresh, GOCDB
  name mapping and change detection.
- **AverageCEEAgent** – maintains rolling averages of CEE per CE based on
  recently finished jobs and stores them in the configuration system.
- **GreenReportingAgent** – collects finished job records, queries CIM/CI
  services for sustainability data, computes energy/emissions figures, and
  persists the metrics to DIRAC databases and ElasticSearch when enabled.
