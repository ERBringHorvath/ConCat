<img src="img/ConCat_V2.jpg" width="100%">

Flexible, Robust Multi-File Concatenation Toolkit

ConCat (short for **concatenate**) is a Python-based command-line tool designed for researchers, bioinformaticians, and data scientists who routinely merge large collections of CSV/TSV/tabular files. ConCat provides:

* Automatic delimiter sniffing (comma, tab, semicolon, pipe)
* Optional delimiter normalization across heterogeneous files
* Strict / union / intersection schema modes
* Column-selection mode with missing-column policies
* Automatic source-file annotation (first column)
* Chunked streaming for multi‑GB files
* Modular architecture similar to SeqForge and ReGAIN

---

# Installation

## Option A — Install via pip (development or user install)

We recommend ConCat be installed in your `~/home` directory

```bash
cd ~
git clone https://github.com/ERBringHorvath/ConCat
cd ~/concat
```

```bash
pip install .
```
Add the following line to the end of `.bashrc`/`.bash_profile`/etc
```bash
export PATH="/home/usr/concat/bin:$PATH"
```
Or add the executable directory to your PATH:

```bash
echo 'export PATH="$HOME/concat/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```
Replace `/home/user/concat/bin` with the actual path to the directory containing the executable.
Whatever the initial directory, this path should end with `/concat/bin`

Close and re-open your terminal or run `source ~/.bashrc` (or whatever your file is named)

Once installed, verify:

```bash
concat --version
```
---

# Usage

ConCat uses a subcommand structure:

```bash
concat <command> [arguments]
```

The primary command is:

```bash
concat combine
```

---

# `concat combine` — Full Argument Reference

## 1. Input Selection (one required)

| Argument                         | Description                                          |
| -------------------------------- | ---------------------------------------------------- |
| `-d DIR`, `--directory DIR`      | Read all files in a directory                        |
| `--glob PATTERN [PATTERN ...]`   | Glob pattern(s) expanded by ConCat (not the shell)   |
| `-i FILE [...]`, `--input-files` | Explicit file paths; works with shell-expanded globs |

---

## 2. File Format & Delimiter Control

| Argument                                 | Description                                            |
| ---------------------------------------- | ------------------------------------------------------ |
| `-e EXT`, `--extension EXT`              | Enforce a specific extension; otherwise all must match |
| `--sample-rows N`                        | Rows used for delimiter sniffing (default 50)          |
| `--normalize {comma,tab,semicolon,pipe}` | Normalize mixed delimiters to a unified one            |

Supported delimiters:

* comma → `,`
* tab → `\t`
* semicolon → `;`
* pipe → `|`

---

## 3. Schema Selection

| Argument                | Description                                           |
| ----------------------- | ----------------------------------------------------- |
| `--schema strict`       | All files must share the exact same columns (default) |
| `--schema union`        | Output includes all columns ever seen in any file     |
| `--schema intersection` | Only output columns shared by all files               |

(*Ignored if `--columns` is used*)

---

## 4. Column Selection Mode

| Argument                               | Description                                     |
| -------------------------------------- | ----------------------------------------------- |
| `--columns COL [...]`                  | Only include the specified columns, in order    |
| `--missing-policy {error,skip,fillna}` | How to handle missing columns in selection mode |
| `--case-insensitive`                   | Case-insensitive column matching                |

Missing policy behaviors:

* `error` → abort
* `skip` → skip the file
* `fillna` → include file, missing values become NA

---

## 5. Source Annotation

| Argument                             | Description                                      |
| ------------------------------------ | ------------------------------------------------ |
| `--no-source-col`                    | Disable automatic first-column source annotation |
| `--source-col-name NAME`             | Rename the source column (default `source_file`) |
| `--source-col-mode {name,stem,path}` | Value in source column                           |

Modes:

* `name` → `file.tsv`
* `stem` → `file`
* `path` → `/full/path/to/file.tsv`

---

## 6. Output Options

| Argument                                 | Description                      |
| ---------------------------------------- | -------------------------------- |
| `-o FILE`, `--out FILE`                  | Output file (required)           |
| `--out-delim {comma,tab,semicolon,pipe}` | Output delimiter (default comma) |
| `--no-header`                            | Omit header row                  |

---

## 7. Performance

| Argument              | Description                           |
| --------------------- | ------------------------------------- |
| `--chunksize N`       | Rows per chunk (default 200,000)      |
| `-T N`, `--threads N` | Threads for normalization (default 4) |

---

## 8. Miscellaneous

| Argument          | Description                         |
| ----------------- | ----------------------------------- |
| `--dry-run`       | Summarize actions but do not output |
| `-V`, `--verbose` | Verbose logging                     |
| `--version`       | Display ConCat version              |

---

# Examples

## Example 1 — Simple directory merge

```bash
concat combine -d ./results/ -e tsv \
  --out merged.tsv --out-delim tab
```

## Example 2 — ConCat-managed glob patterns

```bash
concat combine --glob './runs/*virus_summary.tsv' \
  -o combined_virus_summaries.csv
```

## Example 3 — Shell-expanded input list

```bash
concat combine -i ./data/*.tsv -o merged.tsv
```

## Example 4 — Normalize mixed delimiters

```bash
concat combine --glob './data/*' --normalize tab \
  -o merged.tsv
```

## Example 5 — Union of columns

```bash
concat combine -i FileA.csv FileB.csv FileC.csv --schema union \
  -o merged.csv
```

## Example 6 — Explicit column selection

```bash
concat combine --glob './samples/*.csv' \
  --columns sample_id date score \
  --missing-policy fillna \
  -o combined_subset.csv
```

## Example 7 — Annotate with full file paths

```bash
concat combine -i *.tsv \
  --source-col-mode path \
  -o merged.tsv
```

## Example 8 — Dry run

```bash
concat combine --glob './data/*.tsv' --dry-run
```

---

# Troubleshooting

### "No module named concat.**main**"

Launcher script must point PYTHONPATH to the project root:

```bash
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PYTHONPATH="${PYTHONPATH:-}:${PROJECT_ROOT}"
```

### Inconsistent delimiters

Use:

```bash
--normalize tab
```

Or enforce consistent input.

### Schema mismatch under strict mode

Choose:

```bash
--schema union
```

Or:

```bash
--schema intersection
```

Or explicitly:

```bash
--columns col1 col2 col3
```

---

# License

ConCat is licensed under the **GNU GPLv3 (or later)**.

Full license text: [https://www.gnu.org/licenses/gpl-3.0.txt](https://www.gnu.org/licenses/gpl-3.0.txt)

---

# Cite ConCat

ConCat: Flexible Multi-File Concatenation Toolkit (https://github.com/ERBringHorvath/ConCat)

---

# Contributing

PRs welcome! When contributing:

Keep modules modular (cli.py, combine.py)

Keep behavior consistent with SeqForge/ReGAIN_CLI patterns
