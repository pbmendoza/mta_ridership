# Project Structure

This document gives a simple map of the repository so the top-level README can stay focused on what the project is and how to use it.

## Overview

The repository has one main job: produce monthly subway ridership outputs for stations, PUMAs, and NYC overall.

The current production flow is centered on the API-based pipeline, with older local-processing scripts still kept in the repository as legacy support material.

## Main Entry Point

- `run_pipeline.py` - the supported bootstrap command for first-time and repeat runs

## Main Directories

- `docs/` - user-facing instructions and lightweight project documentation
- `data/` - pipeline inputs, intermediate files, and final outputs
- `scripts/` - pipeline scripts and helper utilities
- `pipelines/` - pipeline runner modules and scheduled-update entry points
- `references/` - source notes, mapping files, and supporting documentation
- `tests/` - automated tests for key pipeline behavior

## Data Directory At A Glance

- `data/api/` - API-derived ridership, baseline, and processed intermediate files
- `data/production/` - final published CSV outputs
- `data/external/` - supporting reference files used by the pipeline

## Scripts Directory At A Glance

- `scripts/api/` - active API-based extraction, aggregation, and baseline scripts
- `scripts/utils/` - shared utilities, including runtime and Socrata helpers
- `scripts/local/` - older local-processing workflows retained for reference
- `scripts/one_off/` - one-time helper scripts
- `scripts/tools/` - validation and support tools

## Notes

- The active production workflow is API-first.
- `scripts/local/` and related local-turnstile workflows are legacy paths, not the primary production route.
- Most users only need `run_pipeline.py` and the instructions in [How to run the pipeline](./run-pipeline.md).
