# **<ins>ConCat<ins/>**

<img src="img/concat_emblem.jpg" width="75%">

ConCat (short for **concatenate**) is a flexible, robust toolkit for combining
tabular files (CSV/TSV/etc.) with:

- Delimiter sniffing and optional normalization
- Strict / union / intersection schema policies
- Optional column selection (`--columns`)
- Automatic source filename column as the first field
- Chunked, streaming concatenation for large files

## Installation

From source:

```bash
pip install .
# or
python -m pip install .

chmod +x concat.sh
./concat.sh combine --help

```
Basic Usage (Directory) <br/>
`concat combine -d ./results/ -e tsv --out combined.tsv --out-delim tab`

Using a Glob Pattern (Handled by ConCat) <br/>
`concat combine --glob './results/*_summary.tsv' --out combined.csv`

Explicit Shell-Expanded Inputs <br/>
`concat combine -i ./results/*.tsv --out combined.csv`

Column Selection with NA Fill <br/>
```
concat combine --glob './results/*_summary.tsv' \
  --columns name date function --missing-policy fillna \
  --out combined_subset.csv
```
