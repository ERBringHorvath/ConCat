# concat/cli.py

from __future__ import annotations

import argparse
import sys

from . import __version__ as CONCAT_VERSION
from . import combine

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="concat",
        description="ConCat: flexible, robust multi-file concatenation toolkit.",
    )

    parser.add_argument("-v" "--version", action="version", version=f"%(prog)s {CONCAT_VERSION}",
                        help="Show version information and exit")

    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # --- combine subcommand ---
    p_comb = subparsers.add_parser(
        "combine",
        help="Combine tabular files (CSV/TSV/etc.) into a single file.",
    )

    inp = p_comb.add_mutually_exclusive_group(required=True)
    inp.add_argument(
        "-d",
        "--directory",
        help="Directory containing input files.",
    )
    inp.add_argument(
        "--glob",
        nargs="+",
        help="Glob pattern(s) for input files, e.g. './data/*_summary.tsv'.",
    )
    inp.add_argument(
        "-i",
        "--input-files",
        nargs="+",
        help="Space-separated list of files to combine.",
    )

    p_comb.add_argument(
        "-e",
        "--extension",
        help=(
            "Expected file extension (e.g., csv, tsv, txt). "
            "If omitted, all inputs must share the same extension."
        ),
        default=None,
    )
    p_comb.add_argument(
        "--sample-rows",
        type=int,
        default=50,
        help="Rows to sample for delimiter sniffing and header peek (default: 50).",
    )
    p_comb.add_argument(
        "--normalize",
        choices=list(combine.SUPPORTED_DELIMS.keys()),
        help="If delimiters are inconsistent, convert inputs to this delimiter in a temp workspace.",
    )
    p_comb.add_argument(
        "--schema",
        choices=["strict", "union", "intersection"],
        default="strict",
        help="How to reconcile columns (default: strict). Ignored if --columns is set.",
    )

    # Column selection
    p_comb.add_argument(
        "--columns",
        nargs="+",
        default=None,
        help=(
            "Only combine these columns (space-separated). Overrides --schema. "
            "Order here determines output order."
        ),
    )
    p_comb.add_argument(
        "--missing-policy",
        choices=["error", "skip", "fillna"],
        default="error",
        help=(
            "When --columns is set and a file is missing requested columns: "
            "'error' (abort), 'skip' (skip file), 'fillna' (include, fill missing as NA)."
        ),
    )
    p_comb.add_argument(
        "--case-insensitive",
        action="store_true",
        help="Match --columns to headers ignoring case.",
    )

    # Source column
    p_comb.add_argument(
        "--no-source-col",
        action="store_true",
        help="Disable the automatic source filename column.",
    )
    p_comb.add_argument(
        "--source-col-name",
        default="source_file",
        help="Name of the source column (default: source_file).",
    )
    p_comb.add_argument(
        "--source-col-mode",
        choices=["name", "stem", "path"],
        default="name",
        help=(
            "What to store in the source column: "
            "'name' = basename with extension (default), "
            "'stem' = basename without extension, 'path' = full path."
        ),
    )

    # Performance / IO
    p_comb.add_argument(
        "--chunksize",
        type=int,
        default=200000,
        help="Rows per chunk when streaming with pandas (default: 200k).",
    )
    p_comb.add_argument(
        "-T",
        "--threads",
        type=int,
        default=4,
        help="Threads to use for normalization step (default: 4).",
    )

    p_comb.add_argument(
        "-o",
        "--out",
        required=True,
        help="Output file path (e.g., combined.csv).",
    )
    p_comb.add_argument(
        "--out-delim",
        choices=list(combine.SUPPORTED_DELIMS.keys()),
        default="comma",
        help="Delimiter for output file (default: comma).",
    )
    p_comb.add_argument(
        "--no-header",
        action="store_true",
        help="Write output without header row.",
    )
    p_comb.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze inputs and print summary without writing output.",
    )
    p_comb.add_argument(
        "-V",
        "--verbose",
        action="store_true",
        help="More logging.",
    )

    p_comb.set_defaults(func=combine.run)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        parser.exit(1)

    try:
        args.func(args)
    except KeyboardInterrupt:
        combine.eprint("Interrupted.")
        sys.exit(130)
    except Exception as exc:
        combine.eprint(f"ERROR: {exc}")
        sys.exit(1)
