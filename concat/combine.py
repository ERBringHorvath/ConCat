# concat/combine.py

from __future__ import annotations

import csv
import sys
import glob as globlib
import tempfile
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

SUPPORTED_DELIMS = {
    "comma": ",",
    "tab": "\t",          # fixed: real tab, not space
    "semicolon": ";",
    "pipe": "|",
}

SNIFF_CANDIDATES = [",", "\t", ";", "|"]


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


#---- File discovery ----

def collect_paths(
    directory: Optional[str],
    glob_patterns: Optional[list[str]],
    input_files: Optional[list[str]],
) -> list[Path]:
    if directory:
        paths = [p for p in Path(directory).iterdir() if p.is_file()]
    elif glob_patterns:
        paths: list[Path] = []
        for entry in glob_patterns:
            # If shell already expanded, treat as literal path
            if Path(entry).exists():
                paths.append(Path(entry))
            else:
                for hit in globlib.glob(entry):
                    paths.append(Path(hit))
    elif input_files:
        paths = [Path(p) for p in input_files]
    else:
        paths = []

    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")

    # de-dup & sort
    return sorted({p.resolve() for p in paths})


def ensure_single_extension(paths: list[Path], user_ext: Optional[str]) -> str:
    if user_ext:
        return user_ext.lower().lstrip(".")

    exts = {p.suffix.lower().lstrip(".") for p in paths}
    if len(exts) != 1:
        raise ValueError(
            f"Inconsistent extensions detected: {sorted(exts)}. "
            "Use --extension to enforce one, or clean inputs."
        )
    return next(iter(exts))


#---- Delimiter, schema helpers ----

def read_head_lines(path: Path, n: int) -> list[str]:
    lines: list[str] = []
    with path.open("r", newline="", encoding="utf-8", errors="ignore") as fh:
        for _ in range(n):
            line = fh.readline()
            if not line:
                break
            lines.append(line)
    return lines


def sniff_delimiter_from_lines(lines: list[str]) -> str:
    best_delim = None
    best_score = (-1, -1)
    from collections import Counter

    for delim in SNIFF_CANDIDATES:
        counts = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            counts.append(len(ln.split(delim)))
        if not counts:
            continue
        c = Counter(counts)
        mode_val, mode_count = c.most_common(1)[0]
        score = (mode_count, mode_val)
        if score > best_score:
            best_score = score
            best_delim = delim

    if best_delim is None:
        sample = "\n".join(lines)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters="".join(SNIFF_CANDIDATES))
            return dialect.delimiter
        except Exception:
            return ","
    return best_delim


def sniff_file_delimiter(path: Path, sample_rows: int) -> str:
    lines = read_head_lines(path, sample_rows)
    return sniff_delimiter_from_lines(lines)


def peek_header(path: Path, delim: str) -> list[str]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as fh:
        reader = csv.reader(fh, delimiter=delim)
        for row in reader:
            if row and any(cell.strip() for cell in row):
                return [h.strip() for h in row]
    return []


def normalize_one(
    src: Path,
    target_dir: Path,
    from_delim: str,
    to_delim: str,
    header: bool = True,
) -> Path:
    dst = target_dir / src.name
    df_iter = pd.read_csv(
        src,
        sep=from_delim,
        dtype=str,
        chunksize=200_000,
        engine="python",
    )
    mode = "w"
    for idx, chunk in enumerate(df_iter):
        chunk.to_csv(
            dst,
            index=False,
            sep=to_delim,
            header=(header and idx == 0),
            mode=mode,
        )
        mode = "a"
    return dst


def build_schema(headers_list: list[list[str]], policy: str) -> list[str]:
    if policy == "strict":
        base = headers_list[0]
        base_set = set(base)
        for hdrs in headers_list[1:]:
            if set(hdrs) != base_set:
                raise ValueError(
                    "Schema mismatch under --schema strict.\n"
                    f"Base: {base}\nOther: {hdrs}"
                )
        return base
    elif policy == "union":
        all_cols: list[str] = []
        seen: set[str] = set()
        for hdrs in headers_list:
            for h in hdrs:
                if h not in seen:
                    seen.add(h)
                    all_cols.append(h)
        return all_cols
    elif policy == "intersection":
        shared = set(headers_list[0])
        for hdrs in headers_list[1:]:
            shared &= set(hdrs)
        if not shared:
            raise ValueError("No shared columns under --schema intersection.")
        return [h for h in headers_list[0] if h in shared]
    else:
        raise ValueError(f"Unknown schema policy: {policy}")


#---- Column selection helpers ----

def make_header_map(headers: list[str], case_insensitive: bool) -> dict[str, str]:
    if case_insensitive:
        return {h.lower(): h for h in headers}
    return {h: h for h in headers}


def resolve_requested_columns(
    requested: list[str],
    header_map: dict[str, str],
    case_insensitive: bool,
) -> tuple[list[str], list[str]]:
    found: list[str] = []
    missing: list[str] = []
    for r in requested:
        key = r.lower() if case_insensitive else r
        if key in header_map:
            found.append(header_map[key])
        else:
            missing.append(r)
    return found, missing


def source_value_for(path: Path, mode: str) -> str:
    if mode == "name":
        return path.name
    elif mode == "stem":
        return path.stem
    elif mode == "path":
        return str(path)
    return path.name


#---- Core combination ----

def combine_files(
    paths: list[Path],
    per_file_delim: dict[Path, str],
    schema_cols: list[str],
    out_path: Path,
    out_delim: str,
    chunksize: int,
    write_header: bool,
    verbose: bool,
    columns_mode: bool,
    per_file_header_maps: Optional[dict[Path, dict[str, str]]],
    missing_policy: Optional[str],
    add_source_col: bool,
    source_col_name: str,
    source_mode: str,
    ) -> None:
    mode = "w"
    total_rows = 0

    iterator = range(len(paths))
    progress = tqdm(iterator, desc="Combining files", unit="file") if tqdm else iterator

    out_cols = [source_col_name] + schema_cols if add_source_col else list(schema_cols)

    for idx in progress:
        p = paths[idx]
        sep = per_file_delim[p]
        if verbose:
            eprint(f"[COMBINE] {p} (sep={repr(sep)})")

        if columns_mode:
            header_map = per_file_header_maps[p]  # type: ignore[index]
            read_cols = [
                header_map.get(
                    (c.lower() if (c not in header_map and c.lower() in header_map) else c),
                    c,
                )
                for c in schema_cols
            ]
        else:
            read_cols = None

        reader = pd.read_csv(
            p,
            sep=sep,
            dtype=str,
            chunksize=chunksize,
            engine="python",
        )

        for chunk in reader:
            if columns_mode:
                present = [c for c in read_cols if c in chunk.columns]  # type: ignore[arg-type]
                df = chunk[present].copy()
                if missing_policy == "fillna":
                    for outcol, filecol in zip(schema_cols, read_cols):  # type: ignore[arg-type]
                        if filecol not in chunk.columns:
                            df[outcol] = pd.NA
                rename_map = {
                    filecol: outcol
                    for outcol, filecol in zip(schema_cols, read_cols)  # type: ignore[arg-type]
                    if filecol in df.columns
                }
                df.rename(columns=rename_map, inplace=True)
                for col in schema_cols:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[schema_cols]
            else:
                for col in schema_cols:
                    if col not in chunk.columns:
                        chunk[col] = pd.NA
                df = chunk[schema_cols]

            if add_source_col:
                df.insert(0, source_col_name, source_value_for(p, source_mode))
                df = df[out_cols]

            df.to_csv(
                out_path,
                sep=out_delim,
                index=False,
                header=(write_header and total_rows == 0),
                mode=mode,
            )
            mode = "a"
            total_rows += len(df)

    if verbose:
        eprint(f"[COMBINE] Wrote {total_rows} rows to {out_path}")


#---- Entrypoint ----

def run(args) -> None:
    """
    Entry point for `concat combine` subcommand.
    Expects an argparse.Namespace from cli.py.
    """
    out_path = Path(args.out)
    out_sep = SUPPORTED_DELIMS[args.out_delim]

    paths = collect_paths(args.directory, args.glob, args.input_files)
    if not paths:
        raise SystemExit("No input files found.")

    ext_norm = ensure_single_extension(paths, args.extension)
    paths = [p for p in paths if p.suffix.lower().lstrip(".") == ext_norm]
    if not paths:
        raise SystemExit(f"No *.{ext_norm} files after filtering. Check inputs/--extension.")

    if args.verbose:
        eprint(f"[INPUT] {len(paths)} files")
        for p in paths:
            eprint(" -", p)

    per_file_delim: dict[Path, str] = {}
    per_file_headers: dict[Path, list[str]] = {}
    for p in paths:
        delim = sniff_file_delimiter(p, args.sample_rows)
        per_file_delim[p] = delim
        per_file_headers[p] = peek_header(p, delim)
        if args.verbose:
            eprint(f"[SNIFF] {p.name}: delim={repr(delim)} | header={per_file_headers[p]}")

    delims = set(per_file_delim.values())
    tmp_workspace: Optional[Path] = None

    try:
        # Normalization if needed
        if len(delims) > 1:
            if args.normalize:
                target = SUPPORTED_DELIMS[args.normalize]
                if args.verbose:
                    eprint(f"[NORMALIZE] Mixed delimiters {delims} -> normalizing to '{args.normalize}'")
                tmp_workspace = Path(tempfile.mkdtemp(prefix="concat_norm_"))
                normalized: list[Path] = []

                with ThreadPoolExecutor(max_workers=args.threads) as ex:
                    futures = [
                        ex.submit(
                            normalize_one,
                            src=p,
                            target_dir=tmp_workspace,
                            from_delim=per_file_delim[p],
                            to_delim=target,
                            header=True,
                        )
                        for p in paths
                    ]
                    results = tqdm(futures, desc="Normalizing", unit="file") if tqdm else futures
                    for fut in results:
                        normalized.append(fut.result())

                paths = sorted(normalized)
                per_file_delim = {p: target for p in paths}
                per_file_headers = {p: peek_header(p, target) for p in paths}
            else:
                opts = ", ".join(SUPPORTED_DELIMS.keys())
                raise SystemExit(
                    f"Inconsistent delimiters detected: {sorted(delims)}. "
                    f"Use --normalize {{{opts}}} to convert."
                )

        # Validate headers
        if any(len(h) == 0 for h in per_file_headers.values()):
            empties = [p.name for p in paths if len(per_file_headers[p]) == 0]
            raise SystemExit(
                f"Could not read header row from: {empties}. Are these empty or malformed?"
            )

        # Column selection vs schema mode
        columns_mode = args.columns is not None and len(args.columns) > 0
        per_file_header_maps: Optional[dict[Path, dict[str, str]]] = None

        if columns_mode:
            requested = args.columns
            per_file_header_maps = {}
            usable_paths: list[Path] = []
            skipped: list[tuple[str, list[str]]] = []

            for p in paths:
                hdrs = per_file_headers[p]
                hmap = make_header_map(hdrs, args.case_insensitive)
                found, missing = resolve_requested_columns(requested, hmap, args.case_insensitive)
                if missing:
                    if args.missing_policy == "error":
                        raise SystemExit(
                            f"File '{p}': missing requested columns {missing} under --missing-policy error"
                        )
                    elif args.missing_policy == "skip":
                        skipped.append((p.name, missing))
                        continue
                    elif args.missing_policy == "fillna":
                        pass
                per_file_header_maps[p] = hmap
                usable_paths.append(p)

            if not usable_paths:
                raise SystemExit("No files left after applying --columns and --missing-policy skip")

            if args.verbose:
                eprint(f"[COLUMNS] requested={requested}")
                if skipped:
                    eprint(f"[COLUMNS] skipped {len(skipped)} files due to missing columns: {skipped}")

            paths = usable_paths
            schema_cols = list(requested)
        else:
            headers_list = [per_file_headers[p] for p in paths]
            schema_cols = build_schema(headers_list, args.schema)
            if args.verbose:
                eprint(f"[SCHEMA] policy={args.schema} -> {len(schema_cols)} columns")
                eprint(f"[SCHEMA] columns={schema_cols}")

        add_source_col = not args.no_source_col
        source_col_name = args.source_col_name
        source_mode = args.source_col_mode

        # Dry run summary
        if args.dry_run:
            eprint("[DRY-RUN] Summary:")
            eprint(f"  Files: {len(paths)}")
            eprint(f"  Extension: .{ext_norm}")
            eprint(f"  Unified delimiter: {repr(next(iter(set(per_file_delim.values()))))}")
            if columns_mode:
                eprint(f"  Columns mode: {schema_cols}")
                eprint(f"  Missing-policy: {args.missing_policy}")
                eprint(f"  Case-insensitive: {args.case_insensitive}")
            else:
                eprint(f"  Schema policy: {args.schema}")
                eprint(f"  Columns: {schema_cols}")
            eprint(
                f"  Source column: {'ON' if add_source_col else 'OFF'} "
                f"| name='{source_col_name}' | mode={source_mode}"
            )
            eprint(f"  Output: {out_path} (delim={args.out_delim}, header={not args.no_header})")
            return

        out_path.parent.mkdir(parents=True, exist_ok=True)

        combine_files(
            paths=paths,
            per_file_delim=per_file_delim,
            schema_cols=schema_cols,
            out_path=out_path,
            out_delim=out_sep,
            chunksize=args.chunksize,
            write_header=(not args.no_header),
            verbose=args.verbose,
            columns_mode=columns_mode,
            per_file_header_maps=per_file_header_maps,
            missing_policy=(args.missing_policy if columns_mode else None),
            add_source_col=add_source_col,
            source_col_name=source_col_name,
            source_mode=source_mode,
        )

        eprint("[DONE] Combined successfully.")
    finally:
        if tmp_workspace and tmp_workspace.exists():
            shutil.rmtree(tmp_workspace, ignore_errors=True)
