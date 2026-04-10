"""
Utilities for incremental saving to BigQuery.
Handles rate limiting and graceful failure recovery.
"""

import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Set, Optional
from utils.bq_utils import save_to_bigquery, load_from_bigquery

# GEE Rate Limits (conservative settings)
# - 40 concurrent requests (default)
# - 100 requests/second (6000/minute)
# - HTTP 429 errors when exceeded
# Conservative approach: stay well below limits

# Sleep times (conservative)
SLEEP_BETWEEN_SUBREGIONS = 8  # Increased from 3s to 8s
SLEEP_BETWEEN_DATASETS = 120  # Increased from 30s to 120s (2 minutes)
SLEEP_AFTER_BATCH = 30  # After every N subregions
BATCH_SIZE = 10  # Save to BQ every N subregions

# Retry settings
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 300  # 5 minutes max

# Stricter settings for daily fetches (more GEE calls, higher rate-limit risk)
MAX_RETRIES_DAILY = 8
SLEEP_BETWEEN_SUBREGIONS_DAILY = 18  # seconds; use for ERA5 daily, VIIRS daily, MODIS
SLEEP_BETWEEN_DATASETS_DAILY = 180   # seconds between ERA5 / MODIS / VIIRS / Terrain

CLIMATOLOGY_ERA5_MAX_WORKERS = 2
CLIMATOLOGY_MODIS_MAX_WORKERS = 3
CLIMATOLOGY_VIIRS_MAX_WORKERS = 5
CLIMATOLOGY_TERRAIN_MAX_WORKERS = 1
CLIMATOLOGY_SLEEP_S = 10


def get_existing_regions(project_id: str, dataset_id: str, table_id: str) -> Set[str]:
    """Get set of regions that already exist in BigQuery table."""
    try:
        query = f"SELECT DISTINCT region FROM `{project_id}.{dataset_id}.{table_id}`"
        df = load_from_bigquery(query, project_id=project_id)
        if df is not None and not df.empty:
            return set(df['region'].unique())
    except Exception as e:
        # Table might not exist yet, that's OK
        pass
    return set()


def save_incremental(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    mode: str = 'WRITE_APPEND',
    retry: bool = True
) -> bool:
    """
    Save DataFrame to BigQuery with retry logic and exponential backoff.
    
    Args:
        df: DataFrame to save
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        mode: Write mode ('WRITE_APPEND' or 'WRITE_TRUNCATE')
        retry: Whether to retry on failure
    
    Returns:
        True if successful, False otherwise
    """
    if df is None or df.empty:
        return False
    
    for attempt in range(MAX_RETRIES if retry else 1):
        try:
            save_to_bigquery(df, project_id, dataset_id, table_id, mode=mode)
            return True
        except Exception as e:
            error_str = str(e).lower()
            is_rate_limit = '429' in error_str or 'quota' in error_str or 'rate limit' in error_str
            
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff with jitter
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                if is_rate_limit:
                    delay = max(delay, 60)  # At least 60s for rate limits
                
                print(f"  ⚠ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                print(f"  Sleeping {delay}s before retry...")
                time.sleep(delay)
            else:
                print(f"  ✗ Failed after {MAX_RETRIES} attempts: {e}")
                return False
    
    return False


def process_with_incremental_save(
    subregions: dict,
    fetch_function,
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str,
    end_date: str,
    fetch_kwargs: Optional[dict] = None,
    skip_existing: bool = True
) -> pd.DataFrame:
    """
    Process subregions with incremental saving to BigQuery.
    
    Args:
        subregions: Dict of {subregion_id: geometry}
        fetch_function: Function to fetch data for a single subregion
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        start_date: Start date for data fetch
        end_date: End date for data fetch
        fetch_kwargs: Additional kwargs to pass to fetch_function
        skip_existing: If True, skip subregions already in BQ
    
    Returns:
        Combined DataFrame of all collected data
    """
    if fetch_kwargs is None:
        fetch_kwargs = {}
    
    # Check existing data in BigQuery
    if skip_existing:
        existing_regions = get_existing_regions(project_id, dataset_id, table_id)
        completed_regions = existing_regions
        print(f"  Existing in BQ: {len(existing_regions)} regions")
    else:
        completed_regions = set()
    
    print(f"  Total completed: {len(completed_regions)} regions")
    print(f"  Remaining: {len(subregions) - len(completed_regions)} regions")
    print()
    
    # Filter out completed regions
    remaining_subregions = {
        rid: geom for rid, geom in subregions.items()
        if rid not in completed_regions
    }
    
    if not remaining_subregions:
        print("  ✓ All subregions already processed!")
        # Load existing data from BQ
        try:
            query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
            return load_from_bigquery(query, project_id=project_id) or pd.DataFrame()
        except:
            return pd.DataFrame()
    
    frames = []
    batch_frames = []
    completed = set(completed_regions)
    total = len(subregions)
    remaining = len(remaining_subregions)
    
    # If not skipping existing, we'll truncate on first save
    is_first_save = not skip_existing
    
    # Create a list of all subregion IDs in original order for correct numbering
    all_subregion_ids = list(subregions.keys())
    
    for i, (subregion_id, geom) in enumerate(remaining_subregions.items(), 1):
        # Calculate global index based on position in original subregions dict
        try:
            global_index = all_subregion_ids.index(subregion_id) + 1
        except ValueError:
            # Fallback if subregion not found (shouldn't happen)
            global_index = len(completed) + i
        
        parent_region = subregion_id.rsplit('_', 1)[0] if '_' in subregion_id else subregion_id
        print(f"[{global_index}/{total}] Processing {subregion_id} (parent: {parent_region})...")
        
        try:
            # Fetch data with retry logic
            df = None
            for attempt in range(MAX_RETRIES):
                try:
                    df = fetch_function(geom, start_date, end_date, **fetch_kwargs)
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    is_rate_limit = '429' in error_str or 'quota' in error_str or 'timeout' in error_str
                    
                    if attempt < MAX_RETRIES - 1:
                        delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                        if is_rate_limit:
                            delay = max(delay, 60)
                        print(f"    ⚠ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                        print(f"    Sleeping {delay}s before retry...")
                        time.sleep(delay)
                    else:
                        raise
            
            if df is not None and not df.empty:
                df["region"] = subregion_id
                df["date"] = pd.to_datetime(df["date"])
                frames.append(df)
                batch_frames.append(df)
                completed.add(subregion_id)
                print(f"  ✓ Collected {len(df)} records")
                print(f"  Batch progress: {len(batch_frames)}/{BATCH_SIZE} subregions")
                
                # Incremental save every BATCH_SIZE subregions
                if len(batch_frames) >= BATCH_SIZE:
                    batch_df = pd.concat(batch_frames, ignore_index=True)
                    print(f"  Saving batch of {len(batch_frames)} subregions to BigQuery...")
                    # Use TRUNCATE on first save if not skipping existing (to overwrite old data)
                    save_mode = 'WRITE_TRUNCATE' if is_first_save else 'WRITE_APPEND'
                    if save_incremental(batch_df, project_id, dataset_id, table_id, mode=save_mode):
                        print(f"  ✓ Batch saved ({len(batch_df)} records)")
                        batch_frames = []  # Clear batch
                        is_first_save = False  # Subsequent saves will append
                    else:
                        print(f"  ⚠ Batch save failed, will retry later")
                    
                    # Extra sleep after batch save
                    print(f"  Sleeping {SLEEP_AFTER_BATCH}s after batch...")
                    time.sleep(SLEEP_AFTER_BATCH)
            else:
                print(f"  ⚠ No data returned")
        except Exception as e:
            print(f"  ✗ Error after {MAX_RETRIES} retries: {e}")
            # Don't add to completed, will retry on next run
        
        # Sleep between subregions (conservative)
        if i < remaining:
            print(f"  Sleeping {SLEEP_BETWEEN_SUBREGIONS}s before next subregion...")
            time.sleep(SLEEP_BETWEEN_SUBREGIONS)
        print()
    
    # Save any remaining batch
    if batch_frames:
        batch_df = pd.concat(batch_frames, ignore_index=True)
        print(f"  Saving final batch of {len(batch_frames)} subregions to BigQuery...")
        save_mode = 'WRITE_TRUNCATE' if is_first_save else 'WRITE_APPEND'
        if save_incremental(batch_df, project_id, dataset_id, table_id, mode=save_mode):
            print(f"  ✓ Final batch saved ({len(batch_df)} records)")
            is_first_save = False
    
    # Combine all frames
    if frames:
        result = pd.concat(frames, ignore_index=True)
        sort_cols = ["region", "date"] if "date" in frames[0].columns else ["region"]
        result = result.sort_values(sort_cols).reset_index(drop=True)
        print(f"Data collection complete. Total new records: {len(result)}")
        
        # Final save of any remaining data (only if we haven't saved yet)
        if is_first_save and len(frames) < BATCH_SIZE:
            print(f"  Saving remaining data to BigQuery...")
            save_incremental(result, project_id, dataset_id, table_id, mode='WRITE_TRUNCATE')
        
        return result
    else:
        print("No new data collected.")
        return pd.DataFrame()


def process_with_per_subregion_save(
    subregions: dict,
    fetch_function,
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str,
    end_date: str,
    fetch_kwargs: Optional[dict] = None,
    skip_existing: bool = False,
    sleep_s: int = SLEEP_BETWEEN_SUBREGIONS_DAILY,
    max_retries: int = MAX_RETRIES_DAILY,
    normalize_date: bool = True,
    max_workers: int = 1,
) -> tuple[pd.DataFrame, bool]:
    """
    Process subregions with save-to-BQ after *every* subregion (append).
    Uses stronger retries and longer sleeps for daily/heavy fetches.

    When max_workers > 1, runs that many regions in parallel (e.g. 2–3 for VIIRS
    when you're the only GEE user). Stays within "too many concurrent aggregations"
    by limiting concurrent fetches. No sleep between regions in parallel mode.

    Returns:
        (result_df, had_regions_to_fetch): had_regions_to_fetch is True if this
        step had any regions to fetch (so caller may sleep before next dataset).
    """
    if fetch_kwargs is None:
        fetch_kwargs = {}

    if skip_existing:
        existing_regions = get_existing_regions(project_id, dataset_id, table_id)
        completed_regions = set(existing_regions)
        print(f"  Existing in BQ: {len(existing_regions)} regions")
    else:
        completed_regions = set()

    remaining_list = [
        (rid, geom) for rid, geom in subregions.items()
        if rid not in completed_regions
    ]

    if not remaining_list:
        print("  All subregions already in BQ. Nothing to fetch.")
        try:
            q = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
            return (load_from_bigquery(q, project_id=project_id) or pd.DataFrame(), False)
        except Exception:
            return (pd.DataFrame(), False)

    frames = []
    total = len(subregions)

    def _fetch_one(rid: str, geom):
        kwargs = {**fetch_kwargs}
        for attempt in range(max_retries):
            try:
                df = fetch_function(geom, start_date, end_date, **kwargs)
                return (rid, df)
            except Exception as e:
                err = str(e).lower()
                is_limit = "429" in err or "quota" in err or "rate limit" in err or "timeout" in err
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                if is_limit:
                    delay = max(delay, 60)
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    print(f"    ✗ Error (after {max_retries} attempts): {e}", flush=True)
                    return (rid, None)
        return (rid, None)

    if max_workers <= 1 or len(remaining_list) <= 1:
        # Sequential: original loop with sleep between regions
        items = list(subregions.items())
        for idx, (subregion_id, geom) in enumerate(items, 1):
            if skip_existing and subregion_id in completed_regions:
                continue
            print(f"  [{idx}/{total}] {subregion_id}...", flush=True)
            _, df = _fetch_one(subregion_id, geom)
            if df is not None and not df.empty:
                df = df.copy()
                df["region"] = subregion_id
                if normalize_date and "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                frames.append(df)
                completed_regions.add(subregion_id)
                if save_incremental(df, project_id, dataset_id, table_id, mode="WRITE_APPEND", retry=True):
                    print(f"  [{idx}/{total}] {subregion_id}: ✓ {len(df)} records appended to BQ", flush=True)
            if idx < total:
                time.sleep(sleep_s)
    else:
        # Parallel: up to max_workers regions at a time, print "[idx/total] region... (fetching)" when starting
        total_remaining = len(remaining_list)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_meta = {}  # future -> (display_idx, rid, parent)
            next_i = 0

            def submit_next():
                nonlocal next_i
                if next_i >= total_remaining:
                    return
                rid, geom = remaining_list[next_i]
                display_idx = next_i + 1
                future = executor.submit(_fetch_one, rid, geom)
                future_to_meta[future] = (display_idx, rid)
                print(f"  [{display_idx}/{total_remaining}] {rid}...", flush=True)
                next_i += 1

            for _ in range(min(max_workers, total_remaining)):
                submit_next()

            pending = set(future_to_meta.keys())
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    display_idx, rid = future_to_meta.pop(future)
                    try:
                        _, df = future.result()
                    except Exception as e:
                        print(f"  [{display_idx}/{total_remaining}] {rid}: ✗ Error: {e}", flush=True)
                        submit_next()
                        continue
                    if df is not None and not df.empty:
                        df = df.copy()
                        df["region"] = rid
                        if normalize_date and "date" in df.columns:
                            df["date"] = pd.to_datetime(df["date"], errors="coerce")
                        frames.append(df)
                        completed_regions.add(rid)
                        save_incremental(df, project_id, dataset_id, table_id, mode="WRITE_APPEND", retry=True)
                        print(f"  [{display_idx}/{total_remaining}] {rid}: ✓ {len(df)} records appended to BQ", flush=True)
                    submit_next()
                # Rebuild pending from future_to_meta so newly submitted futures are included
                pending = set(future_to_meta.keys())

    if not frames:
        return (pd.DataFrame(), True)
    result = pd.concat(frames, ignore_index=True)
    sort_cols = ["region", "date"] if "date" in result.columns else ["region"]
    result = result.sort_values(sort_cols).reset_index(drop=True)
    return (result, True)
