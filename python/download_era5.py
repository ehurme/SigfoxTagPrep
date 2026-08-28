#!/usr/bin/env python3
"""
download_era5.py
================
Download ERA5 reanalysis data for annotating animal movement tracks.

Creates a structured folder of NetCDF files organized by variable type,
pressure level, and month — designed for use with SigfoxTagPrep::annotate_era5().

Output structure:
    <output_dir>/
    ├── single_levels/
    │   ├── era5_single_2024_07.nc      # all surface variables, one month
    │   ├── era5_single_2024_08.nc
    │   └── ...
    ├── pressure_levels/
    │   ├── era5_wind_500hPa_2024_07.nc  # u & v wind at one level, one month
    │   ├── era5_wind_500hPa_2024_08.nc
    │   ├── era5_wind_600hPa_2024_07.nc
    │   └── ...
    └── download_config.json             # copy of config for reproducibility

Usage:
    python download_era5.py era5_config.json
    python download_era5.py era5_config.json --dataset single
    python download_era5.py era5_config.json --dataset pressure
    python download_era5.py era5_config.json --outdir //10.0.16.7/grpdechmann/.../EnvData
    python download_era5.py era5_config.json --verify
    python download_era5.py era5_config.json --verify --retries 3

Requirements:
    pip install cdsapi
    A valid ~/.cdsapirc or environment variables for CDS credentials.
    See: https://cds.climate.copernicus.eu/how-to-api

Part of the SigfoxTagPrep package:
    https://github.com/ehurme/SigfoxTagPrep
"""

import argparse
import calendar
import json
import os
import shutil
import sys
from pathlib import Path, PureWindowsPath

import requests

try:
    import cdsapi
except ImportError:
    sys.exit("cdsapi not installed. Run: pip install cdsapi")

try:
    import xarray as xr
except ImportError:
    xr = None


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_SINGLE_VARS = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "2m_temperature",
    "mean_sea_level_pressure",
    "surface_pressure",
    "total_precipitation",
    "instantaneous_10m_wind_gust",
    "total_cloud_cover",
    "cloud_base_height",
]

DEFAULT_PRESSURE_VARS = [
    "u_component_of_wind",
    "v_component_of_wind",
]

DEFAULT_LEVELS = [500, 600, 700, 800, 850, 900, 925, 950, 1000]

ALL_HOURS = [f"{h:02d}:00" for h in range(24)]


# ── Helpers ──────────────────────────────────────────────────────────────────

def days_in_month(year: int, month: int) -> list[str]:
    """Return zero-padded day strings for every day in the given month."""
    n = calendar.monthrange(year, month)[1]
    return [f"{d:02d}" for d in range(1, n + 1)]


def resolve_path(path_str: str) -> Path:
    """
    Resolve a path string that may be a Windows UNC path (\\\\server\\share)
    or a forward-slash UNC path (//server/share) into a Path object that
    works on the current OS.
    """
    # Normalise forward-slash UNC (from JSON) to backslash UNC on Windows
    if path_str.startswith("//") and os.name == "nt":
        path_str = path_str.replace("/", "\\")
    return Path(path_str)


def load_config(path: str) -> dict:
    """Load and validate a JSON config file."""
    with open(path) as f:
        cfg = json.load(f)

    for key in ("years", "months", "area"):
        if key not in cfg:
            sys.exit(f"Config missing required key: '{key}'")

    area = cfg["area"]
    if len(area) != 4:
        sys.exit("'area' must be [north, west, south, east]")
    if area[0] <= area[2]:
        sys.exit(f"area north ({area[0]}) must be > south ({area[2]})")

    return cfg


def expected_time_steps(year: int, month: int, hours: list[str] | None = None) -> int:
    """Return the expected number of hourly time steps for a month."""
    if hours is None:
        hours = ALL_HOURS
    n_days = calendar.monthrange(year, month)[1]
    return n_days * len(hours)


# Mapping from CDS API variable names to GRIB shortNames.
# Used to verify that every requested variable is actually present in a file.
ERA5_SHORTNAMES = {
    # single levels
    "10m_u_component_of_wind": "10u",
    "10m_v_component_of_wind": "10v",
    "100m_u_component_of_wind": "100u",
    "100m_v_component_of_wind": "100v",
    "2m_temperature": "2t",
    "mean_sea_level_pressure": "msl",
    "surface_pressure": "sp",
    "total_precipitation": "tp",
    "instantaneous_10m_wind_gust": "i10fg",
    "total_cloud_cover": "tcc",
    "cloud_base_height": "cbh",
    # pressure levels
    "u_component_of_wind": "u",
    "v_component_of_wind": "v",
    "temperature": "t",
    "vertical_velocity": "w",
}


def inspect_grib(path: Path) -> tuple[int | None, list[str] | None]:
    """
    Read a GRIB file with eccodes and return (message_count, shortNames).

    shortNames are returned in first-appearance order, which matches the
    layer order the R extractor expects.
    """
    try:
        import eccodes
    except ImportError:
        return None, None

    count = 0
    ordered: list[str] = []
    seen: set[str] = set()
    try:
        with open(path, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                count += 1
                sn = eccodes.codes_get(gid, "shortName", str)
                if sn not in seen:
                    seen.add(sn)
                    ordered.append(sn)
                eccodes.codes_release(gid)
        return count, ordered
    except Exception:
        return None, None


def verify_download(path: Path, expected_steps: int,
                    variables_per_step: int = 1,
                    expected_shortnames: list[str] | None = None) -> tuple[bool, str]:
    """
    Check that a downloaded file is complete and contains the expected variables.

    For GRIB files the total number of messages is compared to the expected
    number (time steps x variables) and, when expected_shortnames is given,
    the layer shortName order is checked. For NetCDF files the 'time' dimension
    is checked directly. Returns (ok, message).
    """
    if not path.exists():
        return False, "file does not exist"
    if path.stat().st_size == 0:
        return False, "file is empty"

    suffix = path.suffix.lower()
    if suffix in (".grib", ".grb", ".grib1", ".grib2"):
        n_messages, actual_shortnames = inspect_grib(path)
        if n_messages is None:
            if xr is None:
                return False, "eccodes/xarray not installed; cannot verify GRIB"
        else:
            expected_messages = expected_steps * variables_per_step
            if n_messages < expected_messages:
                return False, (
                    f"incomplete ({n_messages}/{expected_messages} GRIB messages)"
                )

            if expected_shortnames and actual_shortnames is not None:
                n_expected = len(expected_shortnames)
                if len(actual_shortnames) < n_expected:
                    return False, (
                        f"missing variables: got {actual_shortnames}, "
                        f"expected {expected_shortnames}"
                    )
                if actual_shortnames[:n_expected] != expected_shortnames:
                    return False, (
                        f"wrong layer order/names: got {actual_shortnames}, "
                        f"expected {expected_shortnames}"
                    )
                return True, (
                    f"{n_messages} messages, layers {actual_shortnames[:n_expected]}"
                )
            return True, f"{n_messages} GRIB messages"

    if xr is None:
        return False, "xarray not installed; cannot verify"

    engine = "netcdf4" if suffix in (".nc", ".nc4", ".netcdf") else None
    try:
        ds = xr.open_dataset(str(path), engine=engine, decode_times=True)
        with ds:
            time_coord = ds.coords.get("time") or ds.coords.get("valid_time")
            if time_coord is not None:
                n_steps = len(time_coord)
            else:
                n_steps = ds.sizes.get("time", 0)

            if n_steps < expected_steps:
                return False, f"incomplete ({n_steps}/{expected_steps} time steps)"
            return True, f"{n_steps} time steps"
    except Exception as e:
        return False, f"could not read file: {e}"


# ── Download functions ───────────────────────────────────────────────────────

def download_single_levels(client: cdsapi.Client, cfg: dict, out_root: Path,
                           skip_existing: bool = True,
                           verify_existing: bool = False,
                           max_retries: int = 2):
    """Download ERA5 single-level reanalysis, one file per month."""
    variables = cfg.get("single_level_variables", DEFAULT_SINGLE_VARS)
    area = cfg["area"]
    fmt = cfg.get("data_format", "netcdf")

    out_dir = out_root / "single_levels"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in sorted(cfg["years"], reverse=True):
        for month in sorted(cfg["months"], reverse=True):
            fname = out_dir / f"era5_single_{year}_{month:02d}.grib"
            expected = expected_time_steps(year, month)

            if fname.exists():
                if skip_existing:
                    if verify_existing:
                        ok, msg = verify_download(fname, expected,
                                                  variables_per_step=len(variables))
                        if ok:
                            print(f"  [skip] {fname.name} verified ({msg})")
                            continue
                        print(f"  [warn] {fname.name} {msg}; re-downloading")
                        fname.unlink(missing_ok=True)
                    else:
                        print(f"  [skip] {fname.name} already exists")
                        continue
                else:
                    print(f"  [overwrite] {fname.name}")
                    fname.unlink(missing_ok=True)

            for attempt in range(max_retries + 1):
                print(f"  Requesting single-level {year}-{month:02d} "
                      f"(attempt {attempt + 1}/{max_retries + 1}) ...")
                request = {
                    "product_type": ["reanalysis"],
                    "variable": variables,
                    "year": [str(year)],
                    "month": [f"{month:02d}"],
                    "day": days_in_month(year, month),
                    "time": ALL_HOURS,
                    "data_format": fmt,
                    "download_format": "unarchived",
                    "area": area,
                }
                try:
                    client.retrieve("reanalysis-era5-single-levels", request).download(
                        target=str(fname)
                    )
                    ok, msg = verify_download(fname, expected,
                                              variables_per_step=len(variables))
                    if ok:
                        print(f"  [done] {fname.name} ({msg})")
                        break

                    print(f"  [warn] {fname.name} {msg}")
                    if attempt < max_retries:
                        print("  Retrying...")
                        fname.unlink(missing_ok=True)
                    else:
                        print(f"  [fail] {fname.name} still incomplete after "
                              f"{max_retries} retries")
                        fname.unlink(missing_ok=True)
                except requests.exceptions.HTTPError as e:
                    print(f"  [skip] {year}-{month:02d}: {e}")
                    break


def download_pressure_levels(client: cdsapi.Client, cfg: dict, out_root: Path,
                             skip_existing: bool = True,
                             verify_existing: bool = False,
                             max_retries: int = 2):
    """
    Download ERA5 pressure-level wind data.

    Downloads ONE file per pressure level per month, so every output
    file is a simple 3-D raster [lon x lat x time] that terra::rast()
    reads without needing to parse a level dimension.
    """
    variables = cfg.get("pressure_level_variables", DEFAULT_PRESSURE_VARS)
    levels = cfg.get("pressure_levels", DEFAULT_LEVELS)
    area = cfg["area"]
    fmt = cfg.get("data_format", "netcdf")

    out_dir = out_root / "pressure_levels"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in sorted(cfg["years"], reverse=True):
        for month in sorted(cfg["months"], reverse=True):
            for level in levels:
                fname = out_dir / f"era5_wind_{level}hPa_{year}_{month:02d}.grib"
                expected = expected_time_steps(year, month)

                if fname.exists():
                    if skip_existing:
                        if verify_existing:
                            ok, msg = verify_download(fname, expected,
                                                      variables_per_step=len(variables))
                            if ok:
                                print(f"  [skip] {fname.name} verified ({msg})")
                                continue
                            print(f"  [warn] {fname.name} {msg}; re-downloading")
                            fname.unlink(missing_ok=True)
                        else:
                            print(f"  [skip] {fname.name}")
                            continue
                    else:
                        print(f"  [overwrite] {fname.name}")
                        fname.unlink(missing_ok=True)

                for attempt in range(max_retries + 1):
                    print(f"  Requesting {level} hPa {year}-{month:02d} "
                          f"(attempt {attempt + 1}/{max_retries + 1}) ...")
                    request = {
                        "product_type": ["reanalysis"],
                        "variable": variables,
                        "year": [str(year)],
                        "month": [f"{month:02d}"],
                        "day": days_in_month(year, month),
                        "time": ALL_HOURS,
                        "pressure_level": [str(level)],
                        "data_format": fmt,
                        "download_format": "unarchived",
                        "area": area,
                    }
                    try:
                        client.retrieve(
                            "reanalysis-era5-pressure-levels", request
                        ).download(target=str(fname))
                        ok, msg = verify_download(fname, expected,
                                                  variables_per_step=len(variables))
                        if ok:
                            print(f"  [done] {fname.name} ({msg})")
                            break

                        print(f"  [warn] {fname.name} {msg}")
                        if attempt < max_retries:
                            print("  Retrying...")
                            fname.unlink(missing_ok=True)
                        else:
                            print(f"  [fail] {fname.name} still incomplete after "
                                  f"{max_retries} retries")
                            fname.unlink(missing_ok=True)
                    except requests.exceptions.HTTPError as e:
                        print(f"  [skip] {level} hPa {year}-{month:02d}: {e}")
                        break


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Download ERA5 reanalysis data for SigfoxTagPrep annotation."
    )
    ap.add_argument("config", help="Path to JSON config file")
    ap.add_argument(
        "--dataset",
        choices=["single", "pressure", "both"],
        default="both",
        help="Which dataset(s) to download (default: both)",
    )
    ap.add_argument(
        "--outdir",
        default=None,
        help="Override output_dir from config "
             "(supports UNC paths, e.g. //server/share/era5)",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Re-download files that already exist (default: skip existing)",
    )
    ap.add_argument(
        "--verify",
        action="store_true",
        default=False,
        help="Open existing files and re-download any that are incomplete",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Max retries when a downloaded file is incomplete (default: 2)",
    )
    args = ap.parse_args()

    cfg = load_config(args.config)
    out_root = resolve_path(args.outdir or cfg.get("output_dir", "era5_data"))
    out_root.mkdir(parents=True, exist_ok=True)

    # Save config alongside data for reproducibility
    shutil.copy2(args.config, out_root / "download_config.json")

    client = cdsapi.Client()

    if args.dataset in ("single", "both"):
        n_files = len(cfg["years"]) * len(cfg["months"])
        print(f"=== Single-level variables ({n_files} monthly files) ===")
        download_single_levels(
            client, cfg, out_root,
            skip_existing=not args.overwrite,
            verify_existing=args.verify,
            max_retries=args.retries,
        )

    if args.dataset in ("pressure", "both"):
        levels = cfg.get("pressure_levels", DEFAULT_LEVELS)
        n_files = len(cfg["years"]) * len(cfg["months"]) * len(levels)
        print(f"=== Pressure-level wind ({n_files} files: "
              f"{len(levels)} levels x months) ===")
        download_pressure_levels(
            client, cfg, out_root,
            skip_existing=not args.overwrite,
            verify_existing=args.verify,
            max_retries=args.retries,
        )

    print(f"\nAll files saved to: {out_root}")
    print("Use SigfoxTagPrep::annotate_era5() in R to extract at animal locations.")


if __name__ == "__main__":
    main()
