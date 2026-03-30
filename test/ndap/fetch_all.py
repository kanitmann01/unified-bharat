"""
CLI: fetch all configured NDAP Open API sources with page-based pagination
and write merged JSON snapshots under test/ndap_data/ (and optional CSV).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from ndap_client import DEFAULT_TIMEOUT, fetch_openapi_json

_PLACEHOLDER_PREFIX = "PLACEHOLDER"


def _project_root() -> Path:
    """Repository root: test/ndap/ -> test/ -> project root."""
    return Path(__file__).resolve().parent.parent.parent


def _must_be_under_project_root(project_root: Path, user_path: Path) -> Path:
    """Resolve user_path and reject paths outside project_root (CWE-23)."""
    root = project_root.resolve()
    candidate = user_path.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as e:
        raise ValueError(f"Path must be inside project directory {root}: {candidate}") from e
    return candidate


def _single_output_filename(name: str) -> str:
    """Allow only a plain filename for YAML `output` (no directories or traversal)."""
    if not name or name.strip() != name:
        raise ValueError("Invalid output filename")
    p = Path(name)
    if p.is_absolute() or len(p.parts) != 1 or p.name != name:
        raise ValueError("output must be a single file name with no path components")
    if p.name in (".", "..") or ".." in name:
        raise ValueError("Invalid output filename")
    return p.name


def _load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def _deep_find_first_list_of_dicts(obj: Any, depth: int = 0) -> list[Any] | None:
    """Heuristic: find the first list that looks like tabular rows (dict elements)."""
    if depth > 8:
        return None
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        for item in obj:
            found = _deep_find_first_list_of_dicts(item, depth + 1)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        # NDAP openapi responses use capital-D "Data" for tabular rows; avoid Headers.Items.
        v_data = obj.get("Data")
        if isinstance(v_data, list) and v_data and all(isinstance(x, dict) for x in v_data):
            return v_data
        for key in ("records", "data", "rows", "results", "items"):
            v = obj.get(key)
            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                return v
        for v in obj.values():
            found = _deep_find_first_list_of_dicts(v, depth + 1)
            if found is not None:
                return found
    return None


def _deep_find_first_list(obj: Any, depth: int = 0) -> list[Any] | None:
    if depth > 8:
        return None
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        v_data = obj.get("Data")
        if isinstance(v_data, list):
            return v_data
        for key in ("records", "data", "rows", "results", "items"):
            v = obj.get(key)
            if isinstance(v, list):
                return v
        for v in obj.values():
            found = _deep_find_first_list(v, depth + 1)
            if found is not None:
                return found
    return None


def _row_count(payload: Any) -> int | None:
    """Return a row count when the API exposes a list of records."""
    lst = _deep_find_first_list(payload)
    if lst is not None:
        return len(lst)
    return None


def _parse_positive_int(val: Any) -> int | None:
    """Coerce portal numeric fields that may be int, str, or float."""
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val if val > 0 else None
    if isinstance(val, float):
        try:
            i = int(val)
            return i if i > 0 else None
        except (TypeError, ValueError):
            return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            i = int(float(s))
            return i if i > 0 else None
        except ValueError:
            return None
    return None


def _extract_total_pages(payload: Any) -> int | None:
    """Try common NDAP/portal keys for total page count."""
    if not isinstance(payload, dict):
        return None
    candidates = [
        payload.get("total_pages"),
        payload.get("totalPages"),
        payload.get("Total pages"),
        payload.get("total_page"),
    ]
    for c in candidates:
        n = _parse_positive_int(c)
        if n is not None:
            return n
    return None


def _extract_total_records(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("total_records", "totalRecords", "total_rows", "Total rows", "rows"):
        n = _parse_positive_int(payload.get(key))
        if n is not None:
            return n
    return None


def _primary_indicator_key(source: dict[str, Any]) -> str | None:
    raw = str(source.get("indicators") or "").strip()
    if not raw or raw.startswith(_PLACEHOLDER_PREFIX):
        return None
    first = raw.split(",")[0].strip()
    return first or None


def _nonzero_sum_stats(records: list[dict[str, Any]], indicator_key: str) -> tuple[int, int] | None:
    """Count rows where indicator nested object has numeric sum != 0 (API often returns many zeros)."""
    if not records or not indicator_key:
        return None
    nonzero = 0
    for r in records:
        block = r.get(indicator_key)
        if not isinstance(block, dict):
            continue
        s = block.get("sum")
        if s is None:
            continue
        try:
            if float(s) != 0.0:
                nonzero += 1
        except (TypeError, ValueError):
            continue
    return nonzero, len(records)


def _merge_record_lists(pages: list[Any]) -> list[dict[str, Any]]:
    """Concatenate tabular rows across pages when each page is a dict with a list of row dicts."""
    rows: list[dict[str, Any]] = []
    for page in pages:
        lst = _deep_find_first_list_of_dicts(page)
        if lst is None:
            continue
        for r in lst:
            if isinstance(r, dict):
                rows.append(r)
    return rows


def _flatten_nested_dict(d: dict[str, Any], prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}"
        if isinstance(v, dict):
            out.update(_flatten_nested_dict(v, key))
        elif isinstance(v, (list, tuple)):
            out[key] = json.dumps(v, ensure_ascii=False)
        else:
            out[key] = v
    return out


def _flatten_record_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    """Turn one API row into flat string-keyed dicts for CSV (nested dicts become dotted columns)."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        sk = str(k)
        if isinstance(v, dict):
            out.update(_flatten_nested_dict(v, sk))
        elif isinstance(v, (list, tuple)):
            out[sk] = json.dumps(v, ensure_ascii=False)
        else:
            out[sk] = v
    return out


def _csv_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _write_records_csv(flat_rows: list[dict[str, Any]], path: Path) -> None:
    """Write UTF-8 with BOM so Excel on Windows opens Unicode correctly."""
    fieldnames: list[str] = []
    seen: set[str] = set()
    for r in flat_rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in flat_rows:
            w.writerow({k: _csv_cell(r.get(k)) for k in fieldnames})


def _build_params(
    defaults: dict[str, Any],
    source: dict[str, Any],
    *,
    api_key: str,
    page_no: int,
) -> dict[str, Any]:
    api_key_param = str(defaults.get("api_key_param", "API_Key"))
    indicators_param = str(defaults.get("indicators_param", "Indicators"))
    page_param = str(defaults.get("page_param", "Page No."))

    params: dict[str, Any] = {
        api_key_param: api_key,
        indicators_param: source["indicators"],
        page_param: page_no,
    }
    extra = source.get("extra_params") or {}
    if isinstance(extra, dict):
        params.update(extra)
    return params


def _fetch_source_pages(
    base_url: str,
    defaults: dict[str, Any],
    source: dict[str, Any],
    api_key: str,
    *,
    max_pages: int,
) -> tuple[list[Any], int | None, int | None]:
    """
    Returns (pages_json, total_pages_or_none, total_records_or_none).
    Stops when a page has no rows, when the last requested page is reached,
    or when total_pages (from the API) is satisfied.
    """
    pages: list[Any] = []
    total_pages_hint: int | None = None
    total_records_hint: int | None = None

    page_start = int(defaults.get("page_start", 1))
    page_no = page_start

    while page_no <= max_pages:
        params = _build_params(defaults, source, api_key=api_key, page_no=page_no)
        page_json = fetch_openapi_json(base_url, params, timeout=DEFAULT_TIMEOUT)
        pages.append(page_json)

        if total_pages_hint is None:
            total_pages_hint = _extract_total_pages(page_json)
        if total_records_hint is None:
            total_records_hint = _extract_total_records(page_json)

        rows = _row_count(page_json)
        lst = _deep_find_first_list(page_json)

        if rows is not None and rows == 0:
            if page_no > page_start:
                pages.pop()
            break

        if isinstance(lst, list) and len(lst) == 0 and rows is None:
            if page_no > page_start:
                pages.pop()
            break

        if total_pages_hint is not None and page_no >= total_pages_hint:
            break

        page_no += 1

    return pages, total_pages_hint, total_records_hint


def run(
    *,
    config_path: Path,
    output_dir: Path,
    only_ids: set[str] | None,
    max_pages: int,
    write_csv: bool = True,
) -> int:
    root = _project_root()
    load_dotenv(root / ".env")
    api_key = (os.environ.get("NDAP_API_KEY") or "").strip()
    if not api_key:
        print("ERROR: Set NDAP_API_KEY in the environment or .env file.", file=sys.stderr)
        return 1

    default_base = (os.environ.get("NDAP_BASE_URL") or "").strip()
    if not default_base:
        print("ERROR: Set NDAP_BASE_URL in the environment or .env file.", file=sys.stderr)
        return 1

    cfg = _load_config(config_path)
    defaults = cfg.get("defaults") or {}
    sources = cfg.get("sources") or []
    if not isinstance(sources, list):
        print("ERROR: config 'sources' must be a list.", file=sys.stderr)
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    exit_code = 0
    for source in sources:
        if not isinstance(source, dict):
            continue
        sid = str(source.get("id", ""))
        if only_ids is not None and sid not in only_ids:
            continue

        if not source.get("enabled", True):
            print(f"Skip (disabled): {sid}")
            continue

        ind = str(source.get("indicators", "")).strip()
        if not ind or ind.startswith(_PLACEHOLDER_PREFIX):
            print(f"Skip (placeholder indicators): {sid!r}")
            continue

        base_url = str(source.get("base_url") or default_base).strip()
        try:
            out_name = _single_output_filename(str(source.get("output") or f"{sid}.json"))
        except ValueError as e:
            print(f"ERROR {sid}: {e}", file=sys.stderr)
            exit_code = 1
            continue
        out_path = output_dir / out_name
        try:
            out_path = _must_be_under_project_root(root, out_path)
        except ValueError as e:
            print(f"ERROR {sid}: {e}", file=sys.stderr)
            exit_code = 1
            continue

        print(f"Fetching {sid} ...")
        try:
            pages, total_pages_hint, total_records_hint = _fetch_source_pages(
                base_url,
                defaults,
                source,
                api_key,
                max_pages=max_pages,
            )
        except Exception as e:
            print(f"ERROR {sid}: {e}", file=sys.stderr)
            exit_code = 1
            continue

        records = _merge_record_lists(pages)
        payload = {
            "source_id": sid,
            "title": source.get("title"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "base_url": base_url,
            "indicators": ind,
            "pages_fetched": len(pages),
            "total_pages_hint": total_pages_hint,
            "total_records_hint": total_records_hint,
            "record_count": len(records),
            "pages": pages,
            "records": records,
        }

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"Wrote {out_path} ({len(pages)} page(s), {len(records)} rows merged).")

        pk = _primary_indicator_key(source)
        if pk and records:
            stats = _nonzero_sum_stats(records, pk)
            if stats is not None:
                nz, tot = stats
                print(
                    f"  {pk}: rows with non-zero sum: {nz} / {tot} "
                    "(many zeros are normal; stddev/weights are often null per API.)"
                )

        if write_csv and records:
            try:
                csv_name = _single_output_filename(f"{Path(out_name).stem}.csv")
            except ValueError as e:
                print(f"ERROR {sid} (CSV): {e}", file=sys.stderr)
                exit_code = 1
            else:
                csv_path = output_dir / csv_name
                try:
                    csv_path = _must_be_under_project_root(root, csv_path)
                except ValueError as e:
                    print(f"ERROR {sid} (CSV): {e}", file=sys.stderr)
                    exit_code = 1
                else:
                    flat = [_flatten_record_for_csv(r) for r in records]
                    _write_records_csv(flat, csv_path)
                    print(f"Wrote {csv_path} ({len(records)} row(s)).")

    return exit_code


def main() -> None:
    root = _project_root()
    default_config = root / "test" / "ndap" / "sources.yaml"
    output_dir = root / "test" / "ndap_data"

    parser = argparse.ArgumentParser(
        description="Fetch NDAP Open API sources into test/ndap_data/ JSON (and CSV by default).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=default_config,
        help="Path to sources.yaml (must be under the project directory)",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write a flattened CSV next to each JSON file",
    )
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated source ids to fetch (default: all enabled)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5000,
        help="Safety cap on pagination iterations per source",
    )
    args = parser.parse_args()

    only: set[str] | None = None
    if args.only.strip():
        only = {x.strip() for x in args.only.split(",") if x.strip()}

    try:
        config_path = _must_be_under_project_root(root, args.config)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)

    code = run(
        config_path=config_path,
        output_dir=output_dir,
        only_ids=only,
        max_pages=max(1, args.max_pages),
        write_csv=not args.no_csv,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()
