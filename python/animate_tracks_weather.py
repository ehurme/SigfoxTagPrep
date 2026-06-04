#!/usr/bin/env python3
"""
animate_tracks_weather.py
=========================
Animate animal movement tracks overlaid on ERA5 single-level weather layers.

Creates a meteorology-style animated map showing:
  - MSLP isobar contours (pressure lines, always on by default)
  - Optional 2-m temperature heatmap background
  - Optional total precipitation background
  - Optional 10-m wind barbs
  - Animal tracks with cumulative-reveal paths, coloured by species (or any column)

Input
-----
tracks   — CSV or Parquet file, or a pandas DataFrame with at minimum:
           timestamp, longitude, latitude, individual identifier columns.
era5_dir — Directory produced by download_era5.py, must contain:
           single_levels/era5_single_{year}_{month:02d}.{nc|grib}

Output
------
.gif  — animated GIF  (requires Pillow: pip install Pillow)
.mp4  — H.264 video   (requires ffmpeg accessible from PATH)

Part of the SigfoxTagPrep package:
    https://github.com/ehurme/SigfoxTagPrep
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import xarray as xr
except ImportError:
    sys.exit("xarray not installed.  Run:  pip install xarray netCDF4")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    from matplotlib.animation import FuncAnimation, PillowWriter, FFMpegWriter
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
except ImportError:
    sys.exit("matplotlib not installed.  Run:  pip install matplotlib")

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
except ImportError:
    sys.exit("cartopy not installed.  Run:  pip install cartopy")


# ── Default colour palettes ───────────────────────────────────────────────────

_DEFAULT_PALETTE = [
    "#e41a1c", "#377eb8", "#4daf4a", "#ff7f00",
    "#984ea3", "#a65628", "#f781bf", "#ffff33",
    "#00e5ff", "#ff6b35", "#b5e61d", "#c8a2c8",
]

# Species shortname map for cleaner legend labels
_SPECIES_SHORT = {
    "Nyctalus noctula":     "N. noctula",
    "Nyctalus leisleri":    "N. leisleri",
    "Nyctalus lasiopterus": "N. lasiopterus",
}


# ═══════════════════════════════════════════════════════════════════════════════
# ERA5 helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _find_single_level_files(era5_dir: Path, date_range: pd.DatetimeIndex) -> tuple[list[str], str]:
    """Return (paths, engine) for single-level ERA5 files covering *date_range*.
    Supports NetCDF (.nc) and GRIB (.grib) — whichever is present.
    """
    single_dir = era5_dir / "single_levels"
    if not single_dir.exists():
        sys.exit(
            f"ERA5 single_levels/ directory not found inside: {era5_dir}\n"
            "Run download_era5.py first."
        )

    nc_count   = len(list(single_dir.glob("*.nc")))
    grib_count = len(list(single_dir.glob("*.grib")))
    if grib_count >= nc_count:
        ext, engine = ".grib", "cfgrib"
    else:
        ext, engine = ".nc", "netcdf4"

    year_months = sorted({(dt.year, dt.month) for dt in date_range})
    files = []
    for y, m in year_months:
        f = single_dir / f"era5_single_{y}_{m:02d}{ext}"
        if f.exists():
            files.append(str(f))
        else:
            print(f"  [warn] ERA5 file not found: {f.name}", file=sys.stderr)

    if not files:
        sys.exit(
            "No ERA5 single-level files found for the track date range "
            f"({date_range.min().date()} – {date_range.max().date()})."
        )
    return files, engine


def _cfgrib_reindex(piece: xr.Dataset) -> xr.Dataset:
    """
    Reindex a single cfgrib hypercube so it is indexed by *valid_time*
    rather than (reference_time, step).

    cfgrib stores ERA5 data with:
      - ``time``       = forecast reference time  (not the valid datetime)
      - ``step``       = timedelta from reference to valid time
      - ``valid_time`` = time + step              (the actual datetime we want)

    This function collapses (time, step) → valid_time so the returned dataset
    has a single 1-D ``time`` dimension containing actual valid datetimes.
    """
    if "step" not in piece.dims:
        # No step dim — just swap valid_time in if present
        if "valid_time" in piece.coords and "time" in piece.dims:
            vt = piece["valid_time"].values
            if vt.ndim == 1 and len(vt) == piece.sizes["time"]:
                piece = piece.assign_coords(time=vt)
        return piece

    if piece.sizes["step"] == 1:
        # Single step value — squeeze it and use valid_time as the index
        piece = piece.isel(step=0, drop=True)
        if "valid_time" in piece.coords:
            vt = piece["valid_time"].values
            if vt.ndim == 1 and len(vt) == piece.sizes["time"]:
                piece = piece.assign_coords(time=vt)
        return piece

    # Multiple step values: stack (time × step) → flat record dimension,
    # assign valid_time as coordinate, drop NaT, sort, deduplicate.
    piece = piece.stack(record=("time", "step"))
    vt = pd.to_datetime(piece["valid_time"].values)
    piece = piece.assign_coords(record=vt)
    # Drop records with no valid time (NaT)
    ok = ~pd.isnull(vt)
    piece = piece.isel(record=np.where(ok)[0])
    vt_ok = vt[ok]
    # Sort chronologically, keep first occurrence of each valid_time
    # (prefers lower step values, i.e. instantaneous > accumulated)
    order = np.argsort(vt_ok, kind="stable")
    piece = piece.isel(record=order)
    vt_sorted = vt_ok[order]
    _, first = np.unique(vt_sorted, return_index=True)
    piece = piece.isel(record=first)
    piece = piece.rename({"record": "time"})
    piece = piece.assign_coords(time=vt_sorted[first])
    return piece


def load_era5_single(era5_dir: Path, date_range: pd.DatetimeIndex,
                     verbose: bool = True) -> xr.Dataset:
    """
    Open ERA5 single-level files (NetCDF or GRIB) as an xarray Dataset
    indexed by actual valid datetimes with coordinate names lon/lat/time.
    """
    files, engine = _find_single_level_files(era5_dir, date_range)
    print(f"  Loading {len(files)} ERA5 file(s) ({engine}) …")

    if engine == "cfgrib":
        try:
            import cfgrib
        except ImportError:
            sys.exit("cfgrib not installed.  Run:  pip install cfgrib")

        # cfgrib splits multi-message GRIBs into one dataset per hypercube.
        # Reindex each piece by valid_time, then merge all variables together.
        xr.set_options(use_new_combine_kwarg_defaults=True)  # silence FutureWarning
        reindexed = []
        for f in files:
            try:
                for piece in cfgrib.open_datasets(f, indexpath=""):
                    reindexed.append(_cfgrib_reindex(piece))
            except Exception as e:
                print(f"  [warn] could not open {Path(f).name}: {e}", file=sys.stderr)

        if not reindexed:
            sys.exit("cfgrib returned no datasets from the GRIB files.")

        ds = xr.merge(reindexed, compat="override", join="outer")

    else:
        ds = xr.open_mfdataset(files, combine="by_coords", engine="netcdf4")

    # ── Normalise coordinate names ────────────────────────────────────────────
    renames = {}
    for old, new in [("longitude", "lon"), ("latitude", "lat")]:
        if old in ds.dims:
            renames[old] = new
    if renames:
        ds = ds.rename(renames)

    # Remap 0–360 longitudes to −180…180
    if "lon" in ds.coords and float(ds["lon"].max()) > 180:
        ds = ds.assign_coords(lon=((ds["lon"].values + 180) % 360) - 180)
        ds = ds.sortby("lon")

    if verbose:
        print(f"  ERA5 variables: {list(ds.data_vars)}")
        if "time" in ds.dims:
            t_vals = pd.to_datetime(ds["time"].values)
            ok = t_vals[~pd.isnull(t_vals)]
            if len(ok):
                print(f"  ERA5 time:      {ok[0]:%Y-%m-%d %H:%M} – "
                      f"{ok[-1]:%Y-%m-%d %H:%M}  ({ds.sizes['time']} steps)")

    return ds


# ═══════════════════════════════════════════════════════════════════════════════
# Track loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_tracks(
    source,
    time_col: str = "timestamp",
    lon_col:  str = "location_long",
    lat_col:  str = "location_lat",
    id_col:   str = "individual_local_identifier",
    color_col: str | None = None,
) -> pd.DataFrame:
    """
    Load animal tracks from a CSV/Parquet path or a pandas DataFrame.

    Returns a DataFrame with columns:
        time          — UTC-aware Timestamp
        lon, lat      — float coordinates
        individual_id — string tag/individual identifier (used for path grouping)
        color_id      — string used for colour assignment (= color_col if given,
                        else same as individual_id)
    """
    if isinstance(source, (str, Path)):
        p = Path(source)
        df = pd.read_parquet(p) if p.suffix.lower() == ".parquet" else pd.read_csv(p)
    else:
        df = source.copy()

    def _pick(candidates: list[str], purpose: str) -> str:
        for c in candidates:
            if c in df.columns:
                return c
        raise ValueError(
            f"Cannot find {purpose} column. "
            f"Tried: {candidates}\nAvailable: {list(df.columns)}"
        )

    time_col = _pick([time_col, "timestamp", "time", "datetime", "date_time"], "timestamp")
    lon_col  = _pick([lon_col, "location_long", "longitude", "lon", "long", "x"], "longitude")
    lat_col  = _pick([lat_col, "location_lat",  "latitude",  "lat",        "y"], "latitude")
    id_col   = _pick([id_col, "individual_local_identifier", "individual",
                      "id", "animal_id", "tag_id", "tag_local_identifier"], "individual ID")

    ts = pd.to_datetime(df[time_col], utc=True)
    out = pd.DataFrame({
        "time":          ts,
        "lon":           pd.to_numeric(df[lon_col], errors="coerce"),
        "lat":           pd.to_numeric(df[lat_col], errors="coerce"),
        "individual_id": df[id_col].astype(str),
    })

    # colour grouping column (e.g. species)
    if color_col and color_col in df.columns:
        out["color_id"] = df[color_col].astype(str)
    else:
        out["color_id"] = out["individual_id"]

    out = out.dropna(subset=["time", "lon", "lat"]).sort_values("time").reset_index(drop=True)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Main animation function
# ═══════════════════════════════════════════════════════════════════════════════

def animate_tracks_weather(
    tracks,
    era5_dir: str | Path,
    out_file: str = "track_weather_animation.gif",
    # ── Track column names ───────────────────────────────────────────────────
    time_col:  str = "timestamp",
    lon_col:   str = "location_long",
    lat_col:   str = "location_lat",
    id_col:    str = "individual_local_identifier",
    color_col: str | None = None,   # column to colour by (e.g. "species")
    # ── Track styling ────────────────────────────────────────────────────────
    palette: list[str] | None = None,
    point_size:      float = 40,
    path_linewidth:  float = 1.2,
    path_alpha:      float = 0.85,
    # ── Weather layers ───────────────────────────────────────────────────────
    show_pressure:     bool = True,
    show_temperature:  bool = False,
    show_precipitation: bool = False,
    show_wind_barbs:   bool = False,
    # ── Pressure / isobar options ────────────────────────────────────────────
    isobar_interval:      int   = 4,
    isobar_bold_interval: int   = 20,
    isobar_color:         str   = "white",
    isobar_linewidth:     float = 0.7,
    isobar_label_size:    int   = 7,
    # ── Temperature options ──────────────────────────────────────────────────
    temp_cmap:    str          = "RdYlBu_r",
    temp_alpha:   float        = 0.75,
    temp_vmin:    float | None = None,
    temp_vmax:    float | None = None,
    # ── Precipitation options ────────────────────────────────────────────────
    precip_cmap:           str         = "YlGnBu",
    precip_alpha:          float       = 0.65,
    precip_threshold_mm:   float       = 0.05,
    precip_levels: list[float] | None  = None,
    # ── Wind barb options ────────────────────────────────────────────────────
    wind_density:    int   = 6,
    wind_barb_color: str   = "#cccccc",
    wind_barb_length: float = 5.0,
    # ── Scale bar ────────────────────────────────────────────────────────────
    scale_bar_km: int = 500,
    # ── Map extent ───────────────────────────────────────────────────────────
    xlim:   tuple[float, float] | None = None,
    ylim:   tuple[float, float] | None = None,
    buffer: float = 2.0,
    # ── Visual theme ─────────────────────────────────────────────────────────
    dark_theme: bool = True,
    # ── Animation parameters ─────────────────────────────────────────────────
    time_resolution_hours: int = 6,
    fps:       int = 8,
    width_px:  int = 1200,
    height_px: int = 800,
    dpi:       int = 100,
    title_format: str = "{time:%Y-%m-%d %H:%M UTC}",
    verbose: bool = True,
) -> str:
    """
    Render an animated map of animal tracks overlaid on ERA5 weather layers.

    Parameters
    ----------
    tracks : str | Path | pd.DataFrame
        Animal location data.
    era5_dir : str | Path
        Root of the ERA5 data directory (must contain single_levels/).
    color_col : str, optional
        Column in *tracks* to use for colour assignment, e.g. ``"species"``.
        Paths are still drawn per individual; colours are grouped by this column.
    scale_bar_km : int
        Approximate scale bar length in km (default 500).
    """
    era5_dir = Path(era5_dir)

    # ── Load tracks ───────────────────────────────────────────────────────────
    if verbose:
        print("Loading tracks …")
    df = load_tracks(tracks, time_col=time_col, lon_col=lon_col,
                     lat_col=lat_col, id_col=id_col, color_col=color_col)
    if len(df) == 0:
        sys.exit("No valid track points after cleaning.")
    if verbose:
        print(f"  {len(df)} fixes | {df['individual_id'].nunique()} individual(s) | "
              f"{df['time'].min().date()} – {df['time'].max().date()}")

    # ── Load ERA5 ─────────────────────────────────────────────────────────────
    if verbose:
        print("Loading ERA5 single-level data …")
    dr = pd.date_range(
        df["time"].min() - pd.Timedelta(hours=2),
        df["time"].max() + pd.Timedelta(hours=2),
        freq="1h",
    )
    ds = load_era5_single(era5_dir, dr, verbose=verbose)

    # ── Spatial extent ────────────────────────────────────────────────────────
    if xlim is None:
        xlim = (float(df["lon"].min()) - buffer, float(df["lon"].max()) + buffer)
    if ylim is None:
        ylim = (float(df["lat"].min()) - buffer, float(df["lat"].max()) + buffer)

    era5_margin = 3.0
    lat_vals = ds["lat"].values
    if lat_vals[0] > lat_vals[-1]:  # descending (N→S, typical ERA5)
        ds = ds.sel(
            lon=slice(xlim[0] - era5_margin, xlim[1] + era5_margin),
            lat=slice(ylim[1] + era5_margin, ylim[0] - era5_margin),
        )
    else:
        ds = ds.sel(
            lon=slice(xlim[0] - era5_margin, xlim[1] + era5_margin),
            lat=slice(ylim[0] - era5_margin, ylim[1] + era5_margin),
        )

    lons = ds["lon"].values.astype(float)
    lats = ds["lat"].values.astype(float)
    LON, LAT = np.meshgrid(lons, lats)

    # ── Pre-compute temperature range ─────────────────────────────────────────
    if show_temperature and "t2m" in ds:
        if temp_vmin is None or temp_vmax is None:
            if verbose:
                print("  Computing temperature range …")
            t2m_all = ds["t2m"].values - 273.15
            if temp_vmin is None:
                temp_vmin = float(np.nanpercentile(t2m_all, 2))
            if temp_vmax is None:
                temp_vmax = float(np.nanpercentile(t2m_all, 98))
            if verbose:
                print(f"  t2m range: {temp_vmin:.1f} – {temp_vmax:.1f} °C")

    # ── Pre-compute precipitation levels ─────────────────────────────────────
    if precip_levels is None:
        precip_levels = [precip_threshold_mm, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0]

    # ── Isobar levels (computed once over all time steps) ─────────────────────
    if show_pressure and "msl" in ds:
        msl_arr = ds["msl"].values / 100.0
        p_min = float(np.nanpercentile(msl_arr, 1))
        p_max = float(np.nanpercentile(msl_arr, 99))
        lo = int(np.floor(p_min / isobar_interval) * isobar_interval)
        hi = int(np.ceil(p_max  / isobar_interval) * isobar_interval)
        isobar_lvls = list(range(lo, hi + isobar_interval, isobar_interval))
        if verbose:
            print(f"  MSLP range: {p_min:.0f} – {p_max:.0f} hPa, "
                  f"{len(isobar_lvls)} isobar levels")
    else:
        isobar_lvls = []
        if show_pressure and verbose:
            print("  [warn] 'msl' not found in ERA5 dataset — pressure layer skipped")

    # ── Frame times ───────────────────────────────────────────────────────────
    era5_times = pd.DatetimeIndex(ds["time"].values).tz_localize("UTC")
    step = pd.Timedelta(hours=time_resolution_hours)
    raw_frames = pd.date_range(df["time"].min(), df["time"].max(), freq=step)

    frame_era5_idx, frame_times = [], []
    seen: set[int] = set()
    for ft in raw_frames:
        idx = int(np.argmin(np.abs(era5_times - ft)))
        if idx not in seen:
            seen.add(idx)
            frame_era5_idx.append(idx)
            frame_times.append(era5_times[idx])

    n_frames = len(frame_times)
    if verbose:
        print(f"  {n_frames} frames at {time_resolution_hours}h resolution")

    # ── Colour assignment (by color_id, e.g. species) ─────────────────────────
    color_groups = sorted(df["color_id"].unique())
    colors = palette or _DEFAULT_PALETTE
    group_color = {g: colors[i % len(colors)] for i, g in enumerate(color_groups)}
    # Map each individual to its group colour
    ind_to_color = (
        df[["individual_id", "color_id"]]
        .drop_duplicates("individual_id")
        .set_index("individual_id")["color_id"]
        .map(group_color)
        .to_dict()
    )
    individuals = sorted(df["individual_id"].unique())

    # ── Scale bar (precompute degrees) ────────────────────────────────────────
    center_lat = (ylim[0] + ylim[1]) / 2
    scale_deg = scale_bar_km / (111.32 * np.cos(np.radians(center_lat)))

    # ── Theme ─────────────────────────────────────────────────────────────────
    bg     = "#111111" if dark_theme else "#f8f8f8"
    ocean  = "#0d1b2a" if dark_theme else "#d8eaf7"
    border = "#666666" if dark_theme else "#888888"
    txt    = "white"   if dark_theme else "#1a1a1a"
    grid_c = "#333333" if dark_theme else "#cccccc"
    iso_c  = isobar_color

    # ── Figure ────────────────────────────────────────────────────────────────
    data_crs = ccrs.PlateCarree()
    fig, ax = plt.subplots(
        figsize=(width_px / dpi, height_px / dpi),
        dpi=dpi,
        subplot_kw={"projection": data_crs},
    )
    fig.patch.set_facecolor(bg)

    # Coastlines/borders only (no land fill) so temperature shows through
    feat_coastline = cfeature.NaturalEarthFeature(
        "physical", "coastline", "50m",
        facecolor="none", edgecolor=border, linewidth=0.7, zorder=6,
    )
    feat_borders = cfeature.NaturalEarthFeature(
        "cultural", "admin_0_countries", "50m",
        facecolor="none", edgecolor=border, linewidth=0.35, zorder=6,
    )

    # ── Static legend handles (colour groups / species) ───────────────────────
    def _short(name: str) -> str:
        return _SPECIES_SHORT.get(name, name)

    legend_handles = [
        Line2D([0], [0], marker="o", linestyle="-",
               color=group_color[g], markerfacecolor=group_color[g],
               markeredgecolor="none", markersize=5, linewidth=1.2,
               label=_short(g))
        for g in color_groups
    ]
    if show_pressure and isobar_lvls:
        legend_handles.append(
            Line2D([0], [0], color=iso_c, linewidth=0.8, label="MSLP (hPa)")
        )
    if show_temperature and "t2m" in ds:
        legend_handles.append(
            Patch(facecolor=plt.colormaps[temp_cmap](0.5), alpha=0.8, label="2-m Temp (°C)")
        )
    if show_precipitation:
        legend_handles.append(
            Patch(facecolor=plt.colormaps[precip_cmap](0.7), alpha=0.8, label="Precip (mm/h)")
        )

    # ── Per-frame draw ────────────────────────────────────────────────────────
    def draw_frame(fi: int):
        ax.cla()
        ax.set_facecolor(ocean)

        era5_idx = frame_era5_idx[fi]
        t_now = frame_times[fi]
        sl = ds.isel(time=era5_idx)

        # — Temperature heatmap (background, below everything) —
        if show_temperature and "t2m" in sl:
            t2m = sl["t2m"].values.squeeze() - 273.15
            # Ensure (lat, lon) orientation matches meshgrid
            if t2m.shape == LON.T.shape:
                t2m = t2m.T
            ax.pcolormesh(
                LON, LAT, t2m,
                cmap=temp_cmap, alpha=temp_alpha,
                vmin=temp_vmin, vmax=temp_vmax,
                transform=data_crs, zorder=2,
                shading="auto",
            )

        # — Precipitation (above temperature) —
        if show_precipitation and "tp" in sl:
            tp_mm = sl["tp"].values.squeeze() * 1000.0
            if tp_mm.shape == LON.T.shape:
                tp_mm = tp_mm.T
            tp_plot = np.where(tp_mm < precip_threshold_mm, np.nan, tp_mm)
            if not np.all(np.isnan(tp_plot)):
                ax.contourf(
                    LON, LAT, tp_plot,
                    levels=precip_levels, cmap=precip_cmap,
                    alpha=precip_alpha, transform=data_crs,
                    zorder=3, extend="max",
                )

        # — Coastlines & borders (on top of weather layers) —
        ax.add_feature(feat_coastline)
        ax.add_feature(feat_borders)

        # — MSLP isobars —
        if show_pressure and "msl" in sl and isobar_lvls:
            msl_hpa = sl["msl"].values.squeeze() / 100.0
            if msl_hpa.shape == LON.T.shape:
                msl_hpa = msl_hpa.T
            cs = ax.contour(
                LON, LAT, msl_hpa,
                levels=isobar_lvls, colors=iso_c,
                linewidths=isobar_linewidth,
                transform=data_crs, zorder=7,
            )
            bold_lvls = [l for l in isobar_lvls if l % isobar_bold_interval == 0]
            if bold_lvls:
                ax.contour(
                    LON, LAT, msl_hpa,
                    levels=bold_lvls, colors=iso_c,
                    linewidths=isobar_linewidth * 2.2,
                    transform=data_crs, zorder=7,
                )
            ax.clabel(cs, fmt="%d", fontsize=isobar_label_size,
                      colors=iso_c, inline=True, inline_spacing=2)

        # — Wind barbs —
        if show_wind_barbs and "u10" in sl and "v10" in sl:
            n = wind_density
            u10 = sl["u10"].values.squeeze()
            v10 = sl["v10"].values.squeeze()
            if u10.shape == LON.T.shape:
                u10, v10 = u10.T, v10.T
            ax.barbs(
                LON[::n, ::n], LAT[::n, ::n],
                u10[::n, ::n], v10[::n, ::n],
                length=wind_barb_length, color=wind_barb_color,
                linewidth=0.5, transform=data_crs, zorder=8,
                barbcolor=wind_barb_color, flagcolor=wind_barb_color,
            )

        # — Tracks (cumulative reveal) —
        tracks_so_far = df[df["time"] <= t_now]
        for ind in individuals:
            ind_df = tracks_so_far[tracks_so_far["individual_id"] == ind].sort_values("time")
            c = ind_to_color.get(ind, "#ffffff")
            if len(ind_df) > 1:
                ax.plot(
                    ind_df["lon"].values, ind_df["lat"].values,
                    color=c, linewidth=path_linewidth, alpha=path_alpha,
                    transform=data_crs, zorder=9, solid_capstyle="round",
                )
            if len(ind_df) > 0:
                last = ind_df.iloc[-1]
                ax.scatter(
                    last["lon"], last["lat"],
                    color=c, s=point_size,
                    edgecolors="white", linewidths=0.5,
                    transform=data_crs, zorder=10,
                )

        # — Map extent & gridlines —
        ax.set_extent([xlim[0], xlim[1], ylim[0], ylim[1]], crs=data_crs)
        gl = ax.gridlines(
            crs=data_crs, draw_labels=True,
            linewidth=0.3, color=grid_c, linestyle="--", alpha=0.6,
            x_inline=False, y_inline=False,
        )
        gl.top_labels = False
        gl.right_labels = False
        gl.xlocator = mticker.MaxNLocator(5)
        gl.ylocator = mticker.MaxNLocator(5)
        gl.xlabel_style = {"size": 6, "color": txt}
        gl.ylabel_style = {"size": 6, "color": txt}

        # — Scale bar (bottom-right) —
        sb_x1 = xlim[1] - 0.8
        sb_x0 = sb_x1 - scale_deg
        sb_y  = ylim[0] + 0.5
        tick_h = (ylim[1] - ylim[0]) * 0.008
        ax.plot([sb_x0, sb_x1], [sb_y, sb_y],
                color=txt, linewidth=2, transform=data_crs, zorder=11,
                solid_capstyle="butt")
        for xk in [sb_x0, sb_x1]:
            ax.plot([xk, xk], [sb_y - tick_h, sb_y + tick_h],
                    color=txt, linewidth=2, transform=data_crs, zorder=11)
        ax.text((sb_x0 + sb_x1) / 2, sb_y + tick_h * 1.8,
                f"{scale_bar_km} km",
                ha="center", va="bottom", fontsize=6, color=txt,
                transform=data_crs, zorder=11)

        # — Title —
        ax.set_title(
            title_format.format(time=t_now.to_pydatetime()),
            color=txt, fontsize=9, fontweight="bold", pad=5,
        )

        # — Legend (compact) —
        ax.legend(
            handles=legend_handles,
            loc="lower left",
            fontsize=6,
            markerscale=0.85,
            handlelength=1.5,
            handleheight=0.9,
            borderpad=0.5,
            labelspacing=0.3,
            framealpha=0.4,
            labelcolor=txt,
            facecolor=bg,
            edgecolor="#555555",
        )

        if verbose and (fi % 20 == 0 or fi == n_frames - 1):
            print(f"  Frame {fi + 1:>4d}/{n_frames}: {t_now:%Y-%m-%d %H:%M UTC}")

    # ── Render ────────────────────────────────────────────────────────────────
    if verbose:
        print(f"Rendering {n_frames} frames …")

    anim = FuncAnimation(fig, draw_frame, frames=n_frames, blit=False)

    ext = Path(out_file).suffix.lower()
    writer = FFMpegWriter(fps=fps, bitrate=2000) if ext == ".mp4" else PillowWriter(fps=fps)
    anim.save(out_file, writer=writer)
    plt.close(fig)

    if verbose:
        print(f"Saved: {out_file}")

    ds.close()
    return out_file


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Animate animal tracks over ERA5 weather layers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("tracks",   help="CSV or Parquet file of animal fixes")
    ap.add_argument("era5_dir", help="ERA5 data directory (contains single_levels/)")
    ap.add_argument("out_file", help="Output file (.gif or .mp4)")

    ap.add_argument("--time-col",  default="timestamp")
    ap.add_argument("--lon-col",   default="location_long")
    ap.add_argument("--lat-col",   default="location_lat")
    ap.add_argument("--id-col",    default="individual_local_identifier")
    ap.add_argument("--color-col", default=None,
                    help="Column to colour tracks by (e.g. 'species')")

    ap.add_argument("--no-pressure",   dest="show_pressure",     action="store_false")
    ap.add_argument("--temperature",   dest="show_temperature",  action="store_true")
    ap.add_argument("--precipitation", dest="show_precipitation", action="store_true")
    ap.add_argument("--wind-barbs",    dest="show_wind_barbs",   action="store_true")

    ap.add_argument("--isobar-interval", type=int, default=4)
    ap.add_argument("--isobar-color",    default="white")
    ap.add_argument("--scale-bar-km",    type=int, default=500)

    ap.add_argument("--xlim",   nargs=2, type=float, metavar=("W", "E"))
    ap.add_argument("--ylim",   nargs=2, type=float, metavar=("S", "N"))
    ap.add_argument("--buffer", type=float, default=2.0)
    ap.add_argument("--light-theme", dest="dark_theme", action="store_false")

    ap.add_argument("--time-step", type=int, default=6,
                    dest="time_resolution_hours", metavar="HOURS")
    ap.add_argument("--fps",    type=int, default=8)
    ap.add_argument("--width",  type=int, default=1200, dest="width_px")
    ap.add_argument("--height", type=int, default=800,  dest="height_px")
    ap.add_argument("--dpi",    type=int, default=100)
    ap.add_argument("--quiet",  dest="verbose", action="store_false")

    args = vars(ap.parse_args())
    if args["xlim"]:
        args["xlim"] = tuple(args["xlim"])
    if args["ylim"]:
        args["ylim"] = tuple(args["ylim"])

    animate_tracks_weather(**args)


if __name__ == "__main__":
    main()
