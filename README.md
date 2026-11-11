# aind-ephys-curation-pipeline

Re-applies curation to an ephys asset already processed by the
[aind-ephys-pipeline](https://github.com/AllenNeuralDynamics/aind-ephys-pipeline), and writes a curated asset with the
same layout.

| Step | Runs | Does |
|---|---|---|
| `apply_curation` | per stream | removes noise units (UnitRefine / Bombcell labels) and applies the merges of `curated/{stream}/curation.json` |
| `quality_control` | per stream | processing QC only (unit yield, firing rate, curation): no raw data is read |
| `quality_control_collector` | once | new processing QC + the raw-data QC of the processed asset |
| `job_dispatch`, `nwb_units` | once | replaces the units table of the processed asset NWB |

Outputs (`RESULTS_PATH`): `postprocessed/`, `curated/`, `quality_control.json`, `quality_control/`, `nwb/`.

## Inputs

- `--ecephys_url`: the raw session (local path or `s3://`). With `job_dispatch.streaming`, only its metadata is staged.
- `--sorted_url`: the processed asset (local path or `s3://`).

## Parameters

`pipeline/default_params.json`, or `--params_file`. Each step reads its section from `capsule/params.json`:

- `apply_curation.noise_removal_strategy`: `unitrefine`, `bombcell`, `unitrefine&bombcell` (noise for both) or
  `unitrefine|bombcell` (noise for either).
- `apply_curation.censor_ms`: censor period of the merges.
- `apply_curation.merging_mode`: `soft` or `hard` (recomputes the extensions, so the raw data must be readable).

Override with dotted flags (`--apply_curation.merging_mode hard`), or on Code Ocean with
`--extra_params "--apply_curation.merging_mode=hard"`.

Step repos and commits are pinned in `pipeline/process_versions.env` (`process_versions_custom.env` takes priority),
and can be overridden with `--<step>_repo` / `--<step>_version`.

## Running locally

```bash
cd "$(mktemp -d)"
RESULTS_PATH=/path/to/results NXF_SYNTAX_PARSER=v1 nextflow -C <repo>/pipeline/nextflow_local.config \
    run <repo>/pipeline/main.nf --ecephys_url /path/to/raw_session --sorted_url /path/to/processed_asset
```
