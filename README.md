# Gdańsk Live Transit Monitor

A small end-to-end data ingestion platform built on Databricks for the Week 3 team
checkpoint (Labs 1–3). It ingests **Gdańsk public-transport data (Tristar / ZTM Gdańsk)**
into a governed **bronze** layer in Unity Catalog, from both a batch and a streaming source,
and visualises it on a live map dashboard.

## What it does

- **Batch:** static reference data (GTFS schedule, vehicle registry) loaded into bronze,
  idempotently, with metadata columns.
- **Streaming:** the live `gpsPositions` feed ingested into bronze via Auto Loader /
  Event Hub, with schema evolution handled (e.g. a new field appearing mid-stream).
- **Governance:** catalog / schema / volume in Unity Catalog, secrets in a Key Vault-backed scope.
- **Dashboard:** live vehicle positions on a map, plus active-vehicles and delay trends.

## Data source

Tristar open data (CC-BY) published by ZTM Gdańsk — live GPS positions endpoint
(`gpsPositions?v=2`, JSON, ~20s refresh) and static GTFS reference data.

## Structure

```
streaming/   streaming ingestion (gps producer + Auto Loader / Event Hub -> bronze)
batch/       batch ingestion (GTFS + vehicle registry -> bronze)
infra/       Unity Catalog, volume, secret scope setup
```

## Team

| Area | Owner |
|------|-------|
| Infrastructure & governance, repo | @patik1242 |
| Batch ingestion | @ratkoski |
| Streaming ingestion & schema evolution | Gabriela |
| Analytics & demo | shared |

## Working agreement

Work on feature branches, open a pull request into `main`, and get at least one review
per person before merging. Generated data and local environments are not committed
(see `.gitignore`).
