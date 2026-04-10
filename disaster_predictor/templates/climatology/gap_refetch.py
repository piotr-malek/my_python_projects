"""Gap detection and refetch for climatology tables. Refetches missing (region, date) or regions with retries."""

from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from utils.bq_utils import load_from_bigquery
from utils.incremental_save_utils import (
    save_incremental,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    SLEEP_BETWEEN_DATASETS_DAILY,
    CLIMATOLOGY_VIIRS_MAX_WORKERS,
    CLIMATOLOGY_MODIS_MAX_WORKERS,
    CLIMATOLOGY_SLEEP_S,
)

REFETCH_MAX_RETRIES = 8
REFETCH_SLEEP_BETWEEN_REGIONS = 18


def _date_series(start: str, end: str, daily: bool = True):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    delta = timedelta(days=1) if daily else timedelta(days=16)
    d = s
    while d < e:
        yield d.strftime("%Y-%m-%d")
        d += delta


def _get_date_gaps(
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str,
    end_date: str,
    expected_regions: set[str],
    daily: bool,
    label: str,
) -> list[tuple[str, str]]:
    table = f"`{project_id}.{dataset_id}.{table_id}`"
    q = f"SELECT region, DATE(date) AS date FROM {table} WHERE DATE(date) >= '{start_date}' AND DATE(date) < '{end_date}'"
    try:
        df = load_from_bigquery(q, project_id=project_id)
    except Exception as e:
        print(f"    {label} gap query failed: {e}")
        df = None
    actual = set()
    if df is not None and not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        actual = set(zip(df["region"].astype(str), df["date"].astype(str)))
    expected = {(r, d) for r in expected_regions for d in _date_series(start_date, end_date, daily=daily)}
    return sorted(expected - actual)


def get_viirs_gaps(project_id: str, dataset_id: str, start_date: str, end_date: str, expected_regions: set[str]) -> list[tuple[str, str]]:
    return _get_date_gaps(project_id, dataset_id, "viirs", start_date, end_date, expected_regions, daily=True, label="VIIRS")


def get_era5_gaps(project_id: str, dataset_id: str, start_date: str, end_date: str, expected_regions: set[str]) -> list[tuple[str, str]]:
    return _get_date_gaps(project_id, dataset_id, "era5", start_date, end_date, expected_regions, daily=True, label="ERA5")


def get_modis_gaps(project_id: str, dataset_id: str, start_date: str, end_date: str, expected_regions: set[str]) -> list[tuple[str, str]]:
    return _get_date_gaps(project_id, dataset_id, "modis", start_date, end_date, expected_regions, daily=False, label="MODIS")


def get_terrain_gaps(project_id: str, dataset_id: str, expected_regions: set[str]) -> set[str]:
    table = f"`{project_id}.{dataset_id}.terrain_static`"
    q = f"SELECT DISTINCT region FROM {table}"
    try:
        df = load_from_bigquery(q, project_id=project_id)
        actual = set(df["region"].astype(str).unique()) if df is not None and not df.empty else set()
    except Exception as e:
        print(f"    Terrain gap query failed: {e}")
        actual = set()
    return expected_regions - actual


def _refetch_with_retries(
    fetch_fn,
    geom,
    start_date: str,
    end_date: str,
    fetch_kwargs: dict[str, Any],
    label: str,
) -> pd.DataFrame | None:
    for attempt in range(REFETCH_MAX_RETRIES):
        try:
            df = fetch_fn(geom, start_date, end_date, **fetch_kwargs)
            return df
        except Exception as e:
            err = str(e).lower()
            is_limit = "429" in err or "quota" in err or "rate limit" in err or "timeout" in err
            delay = min(INITIAL_RETRY_DELAY * (2**attempt), MAX_RETRY_DELAY)
            if is_limit:
                delay = max(delay, 60)
            if attempt < REFETCH_MAX_RETRIES - 1:
                print(f"      Refetch {label} attempt {attempt + 1}/{REFETCH_MAX_RETRIES} failed: {e}", flush=True)
                print(f"      Sleeping {delay}s before retry...", flush=True)
                time.sleep(delay)
            else:
                print(f"      ✗ Refetch {label} failed after {REFETCH_MAX_RETRIES} retries: {e}", flush=True)
                return None
    return None


def _refetch_one_viirs_region(
    region: str,
    geom: Any,
    mn: str,
    end_str: str,
    gap_dates: list[str],
    project_id: str,
    dataset_id: str,
) -> None:
    from utils.datasets.viirs_utils import fetch_viirs_daily

    df = _refetch_with_retries(fetch_viirs_daily, geom, mn, end_str, {}, f"VIIRS {region}")
    if df is None or df.empty:
        return
    df = df.copy()
    df["region"] = region
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["_dstr"] = df["date"].dt.strftime("%Y-%m-%d")
    to_append = df[df["_dstr"].isin(gap_dates)].drop(columns=["_dstr"])
    if to_append.empty:
        return
    if save_incremental(to_append, project_id, dataset_id, "viirs", mode="WRITE_APPEND", retry=True):
        print(f"      ✓ {region}: appended {len(to_append)} rows", flush=True)
    else:
        print(f"      ✗ {region}: append failed", flush=True)


def _refetch_viirs_gaps(
    subregions: dict[str, Any],
    gaps: list[tuple[str, str]],
    project_id: str,
    dataset_id: str,
) -> None:
    if not gaps:
        return
    by_region: dict[str, list[str]] = defaultdict(list)
    for r, d in gaps:
        by_region[r].append(d)

    tasks = []
    for region, dates in sorted(by_region.items()):
        geom = subregions.get(region)
        if geom is None:
            continue
        gap_dates = sorted(set(dates))
        mn = gap_dates[0]
        mx = gap_dates[-1]
        end_dt = datetime.strptime(mx, "%Y-%m-%d") + timedelta(days=1)
        end_str = end_dt.strftime("%Y-%m-%d")
        tasks.append((region, geom, mn, end_str, gap_dates))

    if not tasks:
        return

    n_tasks = len(tasks)
    if n_tasks <= 1 or CLIMATOLOGY_VIIRS_MAX_WORKERS <= 1:
        for i, (region, geom, mn, end_str, gap_dates) in enumerate(tasks, 1):
            print(f"    [VIIRS refetch {i}/{n_tasks}] {region} ({len(gap_dates)} days, {mn} → {gap_dates[-1]})", flush=True)
            _refetch_one_viirs_region(region, geom, mn, end_str, gap_dates, project_id, dataset_id)
            if i < n_tasks:
                time.sleep(CLIMATOLOGY_SLEEP_S)
        return

    print(f"    VIIRS refetch: {n_tasks} regions, {CLIMATOLOGY_VIIRS_MAX_WORKERS} workers (no sleep between)", flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=CLIMATOLOGY_VIIRS_MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(
                _refetch_one_viirs_region,
                region, geom, mn, end_str, gap_dates,
                project_id, dataset_id,
            ): (region, len(gap_dates), mn, gap_dates[-1])
            for (region, geom, mn, end_str, gap_dates) in tasks
        }
        for future in as_completed(future_to_task):
            region, n_days, mn, mx = future_to_task[future]
            done += 1
            try:
                future.result()
            except Exception as e:
                print(f"      ✗ {region}: {e}", flush=True)
            print(f"    [VIIRS refetch {done}/{n_tasks}] {region} ({n_days} days, {mn} → {mx}) done", flush=True)


def _refetch_era5_gaps(
    subregions: dict[str, Any],
    gaps: list[tuple[str, str]],
    project_id: str,
    dataset_id: str,
) -> None:
    if not gaps:
        return
    by_region: dict[str, list[str]] = defaultdict(list)
    for r, d in gaps:
        by_region[r].append(d)
    from utils.datasets.era5_utils import fetch_era5_daily

    for i, (region, dates) in enumerate(sorted(by_region.items()), 1):
        geom = subregions.get(region)
        if geom is None:
            continue
        mn = min(dates)
        mx = max(dates)
        end_dt = datetime.strptime(mx, "%Y-%m-%d") + timedelta(days=1)
        end_str = end_dt.strftime("%Y-%m-%d")
        print(f"    [ERA5 refetch {i}/{len(by_region)}] {region} ({mn} → {mx})", flush=True)
        df = _refetch_with_retries(
            fetch_era5_daily, geom, mn, end_str, {}, f"ERA5 {region}"
        )
        if df is None or df.empty:
            continue
        df = df.copy()
        df["region"] = region
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if save_incremental(df, project_id, dataset_id, "era5", mode="WRITE_APPEND", retry=True):
            print(f"      ✓ Appended {len(df)} rows", flush=True)
        else:
            print(f"      ✗ Append failed", flush=True)
        if i < len(by_region):
            time.sleep(REFETCH_SLEEP_BETWEEN_REGIONS)


def _refetch_one_modis_region(
    region: str,
    geom: Any,
    mn: str,
    mx: str,
    gap_dates: list[str],
    project_id: str,
    dataset_id: str,
) -> None:
    from utils.datasets.modis_utils import modis_16day_combined_df

    end_dt = datetime.strptime(mx, "%Y-%m-%d") + timedelta(days=17)
    end_str = end_dt.strftime("%Y-%m-%d")
    df = _refetch_with_retries(
        modis_16day_combined_df, geom, mn, end_str, {"region_id": region}, f"MODIS {region}"
    )
    if df is None or df.empty:
        return
    df = df.copy()
    df["region"] = region
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["_dstr"] = df["date"].dt.strftime("%Y-%m-%d")
    to_append = df[df["_dstr"].isin(gap_dates)].drop(columns=["_dstr"])
    if to_append.empty:
        return
    if save_incremental(to_append, project_id, dataset_id, "modis", mode="WRITE_APPEND", retry=True):
        print(f"      ✓ {region}: appended {len(to_append)} rows", flush=True)
    else:
        print(f"      ✗ {region}: append failed", flush=True)


def _refetch_modis_gaps(
    subregions: dict[str, Any],
    gaps: list[tuple[str, str]],
    project_id: str,
    dataset_id: str,
) -> None:
    if not gaps:
        return
    by_region: dict[str, list[str]] = defaultdict(list)
    for r, d in gaps:
        by_region[r].append(d)

    tasks = []
    for region, dates in sorted(by_region.items()):
        geom = subregions.get(region)
        if geom is None:
            continue
        gap_dates = sorted(set(dates))
        mn = gap_dates[0]
        mx = gap_dates[-1]
        tasks.append((region, geom, mn, mx, gap_dates))

    if not tasks:
        return

    n_tasks = len(tasks)
    if n_tasks <= 1 or CLIMATOLOGY_MODIS_MAX_WORKERS <= 1:
        for i, (region, geom, mn, mx, gap_dates) in enumerate(tasks, 1):
            print(f"    [MODIS refetch {i}/{n_tasks}] {region} ({len(gap_dates)} periods, {mn} → {mx})", flush=True)
            _refetch_one_modis_region(region, geom, mn, mx, gap_dates, project_id, dataset_id)
            if i < n_tasks:
                time.sleep(REFETCH_SLEEP_BETWEEN_REGIONS)
        return

    print(f"    MODIS refetch: {n_tasks} regions, {CLIMATOLOGY_MODIS_MAX_WORKERS} workers (no sleep between)", flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=CLIMATOLOGY_MODIS_MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(
                _refetch_one_modis_region,
                region, geom, mn, mx, gap_dates,
                project_id, dataset_id,
            ): (region, len(gap_dates), mn, mx)
            for (region, geom, mn, mx, gap_dates) in tasks
        }
        for future in as_completed(future_to_task):
            region, n_periods, mn, mx = future_to_task[future]
            done += 1
            try:
                future.result()
            except Exception as e:
                print(f"      ✗ {region}: {e}", flush=True)
            print(f"    [MODIS refetch {done}/{n_tasks}] {region} ({n_periods} periods, {mn} → {mx}) done", flush=True)


def _refetch_terrain_gaps(
    subregions: dict[str, Any],
    gap_regions: set[str],
    project_id: str,
    dataset_id: str,
) -> None:
    if not gap_regions:
        return
    from utils.datasets.terrain_utils import fetch_terrain_data

    for i, region in enumerate(sorted(gap_regions), 1):
        geom = subregions.get(region)
        if geom is None:
            continue
        print(f"    [Terrain refetch {i}/{len(gap_regions)}] {region}", flush=True)
        df = _refetch_with_retries(
            fetch_terrain_data, geom, "2000-01-01", "2000-01-01", {}, f"Terrain {region}"
        )
        if df is None or df.empty:
            continue
        df = df.copy()
        df["region"] = region
        if save_incremental(df, project_id, dataset_id, "terrain_static", mode="WRITE_APPEND", retry=True):
            print(f"      ✓ Appended terrain", flush=True)
        else:
            print(f"      ✗ Append failed", flush=True)
        if i < len(gap_regions):
            time.sleep(REFETCH_SLEEP_BETWEEN_REGIONS)


def run_gap_detection_and_refetch(
    subregions: dict[str, Any],
    project_id: str,
    dataset_id: str,
    *,
    era5_start: str,
    era5_end: str,
    modis_start: str,
    modis_end: str,
    viirs_start: str,
    viirs_end: str,
) -> None:
    """
    Detect gaps (missing dates/regions) in climatology tables, then refetch with retries.
    """
    expected = set(subregions.keys())
    print()
    print("=" * 80)
    print("GAP DETECTION & REFETCH")
    print("=" * 80)
    print()

    # VIIRS: missing (region, date)
    vg = get_viirs_gaps(project_id, dataset_id, viirs_start, viirs_end, expected)
    n_viirs = len(vg)
    print(f"  VIIRS: {n_viirs} missing (region, date) in [{viirs_start}, {viirs_end})")
    if n_viirs > 0:
        print("  Refetching VIIRS gaps...")
        _refetch_viirs_gaps(subregions, vg, project_id, dataset_id)
    if n_viirs > 0:
        print(f"  Sleeping {SLEEP_BETWEEN_DATASETS_DAILY}s before next dataset...")
        time.sleep(SLEEP_BETWEEN_DATASETS_DAILY)
    print()

    # ERA5: missing (region, date)
    eg = get_era5_gaps(project_id, dataset_id, era5_start, era5_end, expected)
    n_era5 = len(eg)
    print(f"  ERA5: {n_era5} missing (region, date) in [{era5_start}, {era5_end})")
    if n_era5 > 0:
        print("  Refetching ERA5 gaps...")
        _refetch_era5_gaps(subregions, eg, project_id, dataset_id)
    if n_era5 > 0:
        print(f"  Sleeping {SLEEP_BETWEEN_DATASETS_DAILY}s before next dataset...")
        time.sleep(SLEEP_BETWEEN_DATASETS_DAILY)
    print()

    # MODIS: missing (region, date)
    mg = get_modis_gaps(project_id, dataset_id, modis_start, modis_end, expected)
    n_modis = len(mg)
    print(f"  MODIS: {n_modis} missing (region, date) in [{modis_start}, {modis_end})")
    if n_modis > 0:
        print("  Refetching MODIS gaps...")
        _refetch_modis_gaps(subregions, mg, project_id, dataset_id)
    if n_modis > 0:
        print(f"  Sleeping {SLEEP_BETWEEN_DATASETS_DAILY}s before next dataset...")
        time.sleep(SLEEP_BETWEEN_DATASETS_DAILY)
    print()

    # Terrain: missing regions
    tg = get_terrain_gaps(project_id, dataset_id, expected)
    n_terrain = len(tg)
    print(f"  Terrain: {n_terrain} missing regions")
    if n_terrain > 0:
        print("  Refetching terrain gaps...")
        _refetch_terrain_gaps(subregions, tg, project_id, dataset_id)
    print()
    print("=" * 80)
    print("GAP REFETCH COMPLETE")
    print("=" * 80)
    print()
