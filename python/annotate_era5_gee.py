#!/usr/bin/env python3
"""
annotate_era5_gee.py
====================
Extract ERA5 reanalysis variables at animal tracking locations using
Google Earth Engine (earthengine-api / geemap).

No local ERA5 files required — values are extracted server-side and only
the point values are returned.

Available bands from ECMWF/ERA5/HOURLY:
  era5_u10   era5_v10    10 m wind (m/s)
  era5_u100  era5_v100   100 m wind (m/s)
  era5_u500  era5_v500   500 hPa wind (m/s)
  era5_u850  era5_v850   850 hPa wind (m/s)
  era5_t2m               2 m temperature (K)
  era5_msl               Mean sea-level pressure (Pa)
  era5_sp                Surface pressure (Pa)
  era5_tp                Total precipitation (m)
  era5_i10fg             Instantaneous 10 m wind gust (m/s)
  era5_tcc               Total cloud cover (0-1)
  era5_cbh               Cloud base height (m)

Usage:
    # Standalone
    python annotate_era5_gee.py tracks.csv tracks_era5.csv
    python annotate_era5_gee.py tracks.csv tracks_era5.csv \\
        --lon location.long --lat location.lat --time timestamp \\
        --project your-gee-project-id

    # From R
    source("R/annotate_era5_gee.R")
    dat_annotated <- annotate_era5_gee(dat)

Authentication (run once in this Python environment):
    import ee
    ee.Authenticate()
    ee.Initialize(project="your-gee-project-id")

Requirements:
    pip install earthengine-api pandas
"""

import argparse
import sys

import numpy as np
import pandas as pd

try:
    import ee
except ImportError:
    sys.exit(
        "earthengine-api not installed.\n"
        "Run: pip install earthengine-api\n"
        "Then authenticate: python -c \"import ee; ee.Authenticate()\""
    )

# ── Band definitions ──────────────────────────────────────────────────────────
# GEE band name  →  output CSV column name
ERA5_BANDS = {
    "u_component_of_wind_10m":    "era5_u10",
    "v_component_of_wind_10m":    "era5_v10",
    "u_component_of_wind_100m":   "era5_u100",
    "v_component_of_wind_100m":   "era5_v100",
    "u_component_of_wind_500hPa": "era5_u500",
    "v_component_of_wind_500hPa": "era5_v500",
    "u_component_of_wind_850hPa": "era5_u850",
    "v_component_of_wind_850hPa": "era5_v850",
    "temperature_2m":              "era5_t2m",
    "mean_sea_level_pressure":     "era5_msl",
    "surface_pressure":            "era5_sp",
    "total_precipitation":         "era5_tp",
    "instantaneous_10m_wind_gust": "era5_i10fg",
    "total_cloud_cover":           "era5_tcc",
    "cloud_base_height":           "era5_cbh",
}

ERA5_SCALE = 27830   # metres ≈ ERA5 native 0.25° resolution


# ── Core extraction function ──────────────────────────────────────────────────

def annotate_era5_gee(
    df,
    lon_col="longitude",
    lat_col="latitude",
    time_col="timestamp",
    max_time_gap_hours=3,
    verbose=True,
):
    """
    Append ERA5 columns to *df* by extracting values at each point's location
    and nearest hourly ERA5 timestep.

    Parameters
    ----------
    df                 : pd.DataFrame with coordinate and timestamp columns
    lon_col            : column name for longitude (decimal degrees, WGS84)
    lat_col            : column name for latitude
    time_col           : column name for timestamps (any format pd.to_datetime accepts)
    max_time_gap_hours : warn when the nearest ERA5 hour is farther than this
    verbose            : print progress to stdout

    Returns
    -------
    pd.DataFrame — original columns plus era5_* columns (NaN where unavailable)
    """

    df = df.copy()

    # ── Parse timestamps ──────────────────────────────────────────────────
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df["_hour"]  = df[time_col].dt.round("h")

    # Warn about large time gaps
    gap_h = (df[time_col] - df["_hour"]).abs().dt.total_seconds() / 3600
    bad   = gap_h > max_time_gap_hours
    if verbose and bad.any():
        print(f"  [warn] {bad.sum()} fixes >{max_time_gap_hours}h from nearest ERA5 step "
              f"(max: {gap_h.max():.1f}h)")

    # Valid rows: non-missing coordinates
    valid = df[lon_col].notna() & df[lat_col].notna()
    if verbose and not valid.all():
        print(f"  {(~valid).sum()} rows with missing coordinates — will be NaN")

    # Initialise output columns
    for col in ERA5_BANDS.values():
        df[col] = np.nan

    # ── ERA5 ImageCollection ──────────────────────────────────────────────
    ic = ee.ImageCollection("ECMWF/ERA5/HOURLY").select(list(ERA5_BANDS.keys()))

    # Pre-filter to data date range (reduces server-side search)
    t_min = df.loc[valid, "_hour"].min()
    t_max = df.loc[valid, "_hour"].max()
    ic = ic.filterDate(
        t_min.strftime("%Y-%m-%dT%H:%M:%S"),
        (t_max + pd.Timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S"),
    )

    unique_hours = sorted(df.loc[valid, "_hour"].unique())
    if verbose:
        print(f"Extracting ERA5: {len(unique_hours)} timestep(s) × {valid.sum()} fix(es)")

    for i, h in enumerate(unique_hours):
        mask = valid & (df["_hour"] == h)
        sub  = df.loc[mask]

        t_start = pd.Timestamp(h).strftime("%Y-%m-%dT%H:%M:%S")
        t_end   = (pd.Timestamp(h) + pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

        img = ic.filterDate(t_start, t_end).first()

        # FeatureCollection — one point per fix, tagged with the DataFrame row index
        features = [
            ee.Feature(
                ee.Geometry.Point([float(row[lon_col]), float(row[lat_col])]),
                {"_row": int(j)},
            )
            for j, row in sub.iterrows()
        ]
        fc = ee.FeatureCollection(features)

        try:
            sampled = img.sampleRegions(
                collection=fc,
                scale=ERA5_SCALE,
                geometries=False,
            )
            result = sampled.getInfo()

            for feat in result.get("features", []):
                props  = feat.get("properties", {})
                row_id = props.get("_row")
                if row_id is not None:
                    for gee_band, out_col in ERA5_BANDS.items():
                        val = props.get(gee_band)
                        if val is not None:
                            df.at[row_id, out_col] = val

        except Exception as exc:
            if verbose:
                print(f"  [warn] {t_start}: {exc}")

        if verbose and ((i + 1) % 20 == 0 or i + 1 == len(unique_hours)):
            print(f"  {i + 1}/{len(unique_hours)} timesteps done")

    df = df.drop(columns=["_hour"])
    if verbose:
        n_filled = df["era5_u10"].notna().sum()
        print(f"Extraction complete — {n_filled}/{valid.sum()} fixes have era5_u10 values")

    return df


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Annotate a tracking CSV with ERA5 variables from Google Earth Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input",    help="Input CSV file")
    parser.add_argument("output",   help="Output CSV file (ERA5 columns appended)")
    parser.add_argument("--lon",     default="longitude",  help="Longitude column name")
    parser.add_argument("--lat",     default="latitude",   help="Latitude column name")
    parser.add_argument("--time",    default="timestamp",  help="Timestamp column name")
    parser.add_argument("--project", default=None,         help="GEE Cloud project ID")
    parser.add_argument("--quiet",   action="store_true",  help="Suppress progress output")
    args = parser.parse_args()

    # ── GEE authentication / initialisation ──────────────────────────────
    init_kwargs = {"project": args.project} if args.project else {}
    try:
        ee.Initialize(**init_kwargs)
    except Exception:
        print("GEE credentials not found — running ee.Authenticate() ...")
        ee.Authenticate()
        ee.Initialize(**init_kwargs)

    # ── Run ───────────────────────────────────────────────────────────────
    df = pd.read_csv(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    df_out = annotate_era5_gee(
        df,
        lon_col=args.lon,
        lat_col=args.lat,
        time_col=args.time,
        verbose=not args.quiet,
    )

    df_out.to_csv(args.output, index=False)
    print(f"Saved {len(df_out)} rows → {args.output}")


if __name__ == "__main__":
    main()
