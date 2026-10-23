# FEWS Archive Forecast

A forecast retriever and/or generator for FEWS. Every provider runs the same pipeline:

1. **read** — fetch source data (provider-specific)
2. **model** — run a local model, if the provider has one (provider-specific; default pass-through)
3. **map** — convert to the provider's **template** (provider-specific → common; default pass-through)
4. **write** — clip, resample, apply constraints and ensemble products, and write for FEWS (common, `core/fews.py`)

```
archive_forecast/
  core/        pipeline.py · provider.py · fews.py · validation.py · decorator.py
  providers/   graphcast_gfs/ · weathernext_ifs/ · weathernext3/ (stub)   each: provider.py · template.py [· model.py]
run/           <provider>.cmd · <provider>.sh   (bundle entry points, copied into dist/<provider>/)
```

## Templates
`providers/<name>/template.py` is the FEWS format agreed with the FEWS stewards for that provider, in FEWS names:
dimension roles (`dims`: `time` required; `x`, `y`, `z`, `member` optional), `levels`, `products` (file product →
dims and fields), `non_negative`, `ensemble` (members/mean product labels), `variables` and `global_attributes`.
The run file is checked against the template before reading, and the mapped data after mapping.

## Adding a provider
Create `archive_forecast/providers/<name>/` with `provider.py` (a subclass of `core.provider.Provider`: set `template`
and `timestep` in hours, implement `read(request)`, override `model`/`map` when needed) and `template.py` (its agreed
FEWS format). Register it in `core/provider.py`; to deploy it, add `requirements_<name>.txt`, `build_<name>.cmd`/`.sh`
and `run/<name>.cmd`/`.sh`. Only the selected provider is imported at run time.

## Build
One script per environment; each clears and rebuilds its environment, then commits and pushes:

| Script | Builds | Requirements | Frozen |
|---|---|---|---|
| `env.cmd` / `.sh` | `build/Python` (`build/python` on POSIX): development environment, every provider, archive_forecast editable | `requirements_env.txt` | `frozen_requirements_env.txt` |
| `build_graphcast_gfs.cmd` / `.sh` | `dist/graphcast_gfs` → `dist/graphcast_gfs.7z` (`.zip` on POSIX) | `requirements_graphcast_gfs.txt` | `frozen_requirements_graphcast_gfs.txt` |
| `build_weathernext_ifs.cmd` / `.sh` | `dist/weathernext_ifs` → `dist/weathernext_ifs.7z` (`.zip` on POSIX) | `requirements_weathernext_ifs.txt` | `frozen_requirements_weathernext_ifs.txt` |

## Run
Deployed: `dist/graphcast_gfs/graphcast_gfs.cmd <FEWS PI run file>`, `dist/weathernext_ifs/weathernext_ifs.cmd <FEWS PI run file>`
(`.sh` on POSIX).
Development: `build/Python/python.exe -m archive_forecast --provider <name> --assets_path models/<model folder> --config_path <run file>`
(set `CDSAPI_RC` to the IFS `.cdsapirc` for weathernext_ifs).
Run file properties: `time_zero_offset_timesteps`, `forecast_length_timesteps` (provider timesteps), `output_parameters`,
`output_folder`, `output_filename`; optional `output_levels`, `output_bounding_box`, `downscaled_grid_cell_size_deg`,
`downscale_method` (only for templates with those dimension roles) and `provider` (must match `--provider` if both given).

## GPU Environment Variables
- `XLA_PYTHON_CLIENT_PREALLOCATE=false`
- `XLA_PYTHON_CLIENT_ALLOCATOR=platform`
## Weights
- https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/index.html#graphcastgfs_weights
- https://console.cloud.google.com/storage/browser/dm_graphcast
## Sources
- https://github.com/astral-sh/python-build-standalone/releases
- https://github.com/NOAA-EMC/graphcast
- https://github.com/google-deepmind/weathernext
