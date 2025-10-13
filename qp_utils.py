# qp_utils.py
from __future__ import annotations
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, Iterable, Literal, Mapping, List
import json
import re
import numpy as np
import pandas as pd
import pyreadstat

# Try to use rich display if in IPython/Jupyter
try:
    from IPython.display import display  # type: ignore
except Exception:  # pragma: no cover
    def display(x):  # noqa: N802
        print(x)

# Public API (exported names)
__all__ = [
    # core
    "expand_vars",
    "load_year",
    # metadata (JSON compact)
    "save_compact_meta",
    "load_compact_meta",
    # labels (JSON-based)
    "labels_for",
    "var_label_for",
    "decode_labels_for_slice",
    "decode_labels",
    # exploration
    "peek_sav_columns",
    "show_value_table_json",
]

# Small helpers
FloatMode = Literal["none", "safe32", "force32"]

def expand_vars(var_templates: Iterable[str], year: int) -> List[str]:
    """
    Expand variable name templates by replacing {yy} with the last two digits of `year`.
    If a template doesn't contain {yy}, append _{yy} at the end.
    """
    yy = f"{year % 100:02d}"
    out: List[str] = []
    for v in var_templates:
        out.append(v.replace("{yy}", yy) if "{yy}" in v else f"{v}_{yy}")
    return out

def _strip_year_tag(name: str, yy: str) -> str:
    """Remove suffix _{yy} when it appears as a standalone tag (followed by _ or end)."""
    return re.sub(fr"_{yy}(?=(_|$))", "", name)

def _normalize_value_labels(d: dict) -> dict:
    """
    JSON-safe: convert mapping keys to strings (e.g., 1 -> "1", 1.0 -> "1.0").
    """
    return {str(k): v for k, v in d.items()}

def _smallest_nullable_int_dtype(xmin: int, xmax: int) -> str:
    if xmin >= -128 and xmax <= 127:
        return "Int8"
    if xmin >= -32768 and xmax <= 32767:
        return "Int16"
    if xmin >= -2147483648 and xmax <= 2147483647:
        return "Int32"
    return "Int64"

def _downcast_integerish(col: pd.Series) -> pd.Series:
    """
    If Series is integer-like (all non-missing values have ~zero fractional part),
    cast to the smallest nullable Int* dtype. Leaves genuine decimals unchanged.
    """
    if col.size == 0 or pd.api.types.is_integer_dtype(col.dtype):
        return col
    tmp = pd.to_numeric(col, errors="coerce") if col.dtype.kind == "O" else col
    if not pd.api.types.is_numeric_dtype(tmp.dtype):
        return col
    nonna = tmp.dropna()
    if nonna.empty:
        return tmp.astype("Int8")
    if pd.api.types.is_float_dtype(nonna.dtype):
        if not ((nonna - nonna.round()).abs() < 1e-12).all():
            return col  # real decimals present
        nonna = nonna.round()
    xmin, xmax = int(nonna.min()), int(nonna.max())
    dtype = _smallest_nullable_int_dtype(xmin, xmax)
    return (tmp.round().astype(dtype) if pd.api.types.is_float_dtype(tmp.dtype)
            else tmp.astype(dtype))

def _downcast_floatish(col: pd.Series, mode: FloatMode = "none") -> pd.Series:
    """
    Optionally downcast float64 → Float32.
    - 'none'    : keep as Float64
    - 'safe32'  : use Float32 if absolute error <= 5e-6 OR if values have <=2 decimals preserved
    - 'force32' : force Float32
    """
    if mode == "none" or not pd.api.types.is_float_dtype(col.dtype):
        return col
    if mode == "force32":
        return col.astype("Float32")

    # safe32 heuristics
    x = col
    # Heuristic A: preserve cents exactly if values have <= 2 decimals
    scaled = (x * 100).round()
    cents_back = (scaled.astype("Float32") / 100).round(2)
    ok = (np.isclose(x.fillna(0), cents_back.fillna(0), atol=5e-6) | x.isna()).all()
    if ok:
        return x.astype("Float32")

    # Heuristic B: general absolute error bound
    x32 = x.astype("Float32")
    err = (x32.astype("Float64") - x).abs()
    return x32 if (err.fillna(0) <= 5e-6).all() else x

# Core: load one year (columns projected, names stripped)
def load_year(
    year: int,
    path: Path,
    cols: Iterable[str] | None = None,
    dtype_hints: Mapping[str, str] | None = None,   # e.g. {"ano_{yy}":"Int16","sexo_{yy}":"Int8"}
    float_downcast: FloatMode = "none",
) -> Tuple[pd.DataFrame, Any]:
    """
    Load a .sav for a given `year`, optionally reading only `cols` and tightening dtypes.

    Behavior:
      - Reads with apply_value_formats=False (keeps numeric codes numeric).
      - Strips the _{yy} tag from column names (e.g., 'sexo_09' -> 'sexo').
      - Ensures any 'ano*' columns equal `year` and uses Int16.
      - Applies `dtype_hints` (accepts {yy} templates).
      - Light auto-downcast: integer-like → smallest Int*, and (optionally) Float64 → Float32.
    """
    if cols is not None:
        cols = list(cols)

    df, meta = pyreadstat.read_sav(
        str(path),
        usecols=cols,
        apply_value_formats=False,
        dates_as_pandas_datetime=True,
        user_missing=True,
    )

    # strip _yy from column names
    yy = f"{year % 100:02d}"
    rename_map = {c: _strip_year_tag(c, yy) for c in df.columns}
    df = df.rename(columns=rename_map)
    # keep reverse map for potential meta lookups
    df.attrs["qp_varmap"] = {v: k for k, v in rename_map.items()}
    df.attrs["qp_year"] = year

    # ensure year columns are correct and compact
    for c in [c for c in df.columns if c.lower().startswith("ano")]:
        vals = pd.to_numeric(df[c], errors="coerce")
        if vals.isna().any() or (vals != year).any():
            df[c] = pd.Series([year] * len(df), dtype="Int16")
        else:
            df[c] = vals.astype("Int16")

    # normalize dtype_hints to stripped names
    def _normalize_hints(hints: Mapping[str, str] | None, yy_: str) -> Dict[str, str]:
        if not hints:
            return {}
        out: Dict[str, str] = {}
        for k, v in hints.items():
            k_explicit = k.replace("{yy}", yy_)
            k_stripped = _strip_year_tag(k_explicit, yy_)
            out[k_stripped] = v
        return out

    hints_norm = _normalize_hints(dtype_hints, yy)

    # apply explicit hints
    for col, d in hints_norm.items():
        if col in df.columns:
            try:
                if d.startswith("Int") and pd.api.types.is_float_dtype(df[col]):
                    df[col] = df[col].round().astype(d)
                else:
                    df[col] = df[col].astype(d)
            except Exception:
                df[col] = _downcast_integerish(df[col])

    # light auto-downcast
    for c in df.columns:
        if c in hints_norm:
            continue
        df[c] = _downcast_integerish(df[c])
        if pd.api.types.is_float_dtype(df[c].dtype):
            df[c] = _downcast_floatish(df[c], mode=float_downcast)

    # small summary
    n_rows, n_cols = df.shape
    mem_gb = df.memory_usage(deep=True).sum() / 1e9
    sel_info = f" (selected {len(cols)} cols)" if cols is not None else ""
    print(f"Loaded {year}{sel_info}: rows={n_rows:,}, cols={n_cols}, approx memory={mem_gb:.2f} GB")
    return df, meta

# Compact JSON metadata (save/load)
def save_compact_meta(meta: Any, year: int, df_with_attrs: pd.DataFrame, outdir: Path = Path("Build/meta")) -> Path:
    """
    Persist compact metadata for a given year, keyed by STRIPPED names:
      - variable_labels: {<stripped_name>: <long label>}
      - value_labels:    {<stripped_name>: {<code as str>: <label>}}
    Uses df_with_attrs.attrs['qp_varmap'] to map stripped <-> original names.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    yy = f"{year % 100:02d}"

    # map stripped -> original
    varmap: Dict[str, str] = df_with_attrs.attrs.get("qp_varmap", {})
    if not varmap:  # fallback
        varmap = {}
        for orig in meta.column_names:
            stripped = _strip_year_tag(orig, yy)
            varmap[stripped] = orig

    # variable labels
    var_labels: Dict[str, str] = {}
    for stripped, orig in varmap.items():
        var_labels[stripped] = meta.column_names_to_labels.get(orig, "")

    # value labels (stripped keys)
    value_labels: Dict[str, Dict[str, str]] = {}
    vv = meta.variable_value_labels or {}
    for orig_var, mapping in vv.items():
        stripped = _strip_year_tag(orig_var, yy)
        value_labels[stripped] = _normalize_value_labels(dict(mapping))

    payload = {
        "year": year,
        "variable_labels": var_labels,
        "value_labels": value_labels,
    }

    outpath = outdir / f"qp_workers_labels_{year}.json"
    outpath.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"Saved compact meta: {outpath}")
    return outpath

def load_compact_meta(year: int, basedir: Path = Path("Build/meta")) -> dict:
    """Load previously saved compact metadata (JSON) for a year."""
    p = basedir / f"qp_workers_labels_{year}.json"
    return json.loads(p.read_text())

# Label lookups & decoding (JSON-based)
def _normalize_code_keys(mapping: dict) -> dict:
    """
    Normalize mapping keys so both '1' and '1.0' work:
    returns a dict containing original keys and int-like aliases.
    """
    out = {}
    for k, v in mapping.items():
        ks = str(k)
        out[ks] = v
        try:
            x = float(ks)
            if x.is_integer():
                out[str(int(x))] = v
        except Exception:
            pass
    return out

def labels_for(var: str, year: int, basedir: Path = Path("Build/meta")) -> dict:
    """
    Return {code_str -> label} for a STRIPPED variable name (e.g., 'sexo') in `year`,
    using the compact JSON. Tolerates legacy JSONs where keys were suffixed (e.g., 'sexo_09').
    """
    meta = load_compact_meta(year, basedir=basedir)
    vl = meta.get("value_labels", {}) or {}
    yy = f"{year % 100:02d}"
    mapping = vl.get(var) or vl.get(f"{var}_{yy}")  # legacy tolerance
    return _normalize_code_keys(mapping) if mapping else {}

def var_label_for(var: str, year: int, basedir: Path = Path("Build/meta")) -> str:
    """Return the long variable label for a STRIPPED variable name in `year`."""
    meta = load_compact_meta(year, basedir=basedir)
    return meta.get("variable_labels", {}).get(var, "")

def decode_labels_for_slice(
    df: pd.DataFrame,
    var: str,                # stripped name in df (e.g., 'sexo')
    year: int,
    basedir: Path = Path("Build/meta"),
    outcol: str | None = None,
    as_category: bool = True,
) -> str:
    """
    Decode df[var] for rows with df['ano']==year using labels from JSON.
    Creates df[outcol] (default: f'{var}_lbl') and returns the column name.
    """
    mapping = labels_for(var, year, basedir=basedir)
    if not mapping:
        raise ValueError(f"No value labels for {var} in {year} (check {basedir}).")

    outcol = outcol or f"{var}_lbl"
    mask = (pd.to_numeric(df["ano"], errors="coerce").astype("Int64") == year)

    # Convert codes to strings to match JSON keys
    codes_str = pd.to_numeric(df.loc[mask, var], errors="coerce").astype("Int64").astype("string")
    labeled = codes_str.replace(mapping)

    if as_category:
        # preserve insertion order of categories
        cats = list(dict.fromkeys(mapping.values()))
        df.loc[mask, outcol] = pd.Categorical(labeled, categories=cats, ordered=False)
    else:
        df.loc[mask, outcol] = labeled

    return outcol

def decode_labels(
    df: pd.DataFrame,
    var: str,
    years: list[int] | None = None,
    basedir: Path = Path("Build/meta"),
    outcol: str | None = None,
    as_category: bool = True,
) -> str:
    """
    Decode labels for `var` across multiple years present in df['ano'].
    Skips years that don't have mappings; returns the created column name.
    """
    years = years or sorted(pd.to_numeric(df["ano"], errors="coerce").dropna().astype(int).unique().tolist())
    outcol = outcol or f"{var}_lbl"
    for y in years:
        try:
            decode_labels_for_slice(df, var, y, basedir=basedir, outcol=outcol, as_category=as_category)
        except ValueError:
            # no labels for this year/var; skip quietly
            pass
    return outcol

# Exploration helpers (no data-row load)
def peek_sav_columns(path: Path) -> pd.DataFrame:
    """
    Return a DataFrame with SPSS variable names, labels, and SPSS types
    without loading any data rows.

    - Prefer meta.variable_types (0=numeric, >0=string width).
    - Fallback to variable_storage_width if present.
    - Also try to surface SPSS formats (e.g., F8.2, A40, DATE10) when we can.
    """
    _df, meta = pyreadstat.read_sav(
        str(path),
        apply_value_formats=False,
        metadataonly=True,
    )

    cols = list(meta.column_names)
    labels = [meta.column_names_to_labels.get(c, "") for c in cols]

    # 1) Type mappings (widths): 0 -> numeric, >0 -> string(width)
    vtypes = getattr(meta, "variable_types", None) or getattr(meta, "variable_storage_width", None)
    if not isinstance(vtypes, dict):
        vtypes = {}

    # 2) Format strings: e.g., "F8.2", "A80", "DATE10" (name varies across versions)
    fmt_map = (
        getattr(meta, "variable_to_format", None)
        or getattr(meta, "variable_format", None)
        or getattr(meta, "formats", None)
    )
    if isinstance(fmt_map, list):
        # align list to column order
        fmt_map = dict(zip(cols, fmt_map))
    if not isinstance(fmt_map, dict):
        fmt_map = {}

    types_out: list[str] = []
    for c in cols:
        w = vtypes.get(c, None)
        fmt = fmt_map.get(c, "")
        if isinstance(w, (int, float)):
            if w > 0:
                types_out.append(f"STRING({int(w)})")
            else:
                # numeric; if we know a specific SPSS format (DATE, Fw.d, etc.) show it
                types_out.append(fmt or "NUMERIC")
        else:
            # no width info; fall back to known format, else empty
            types_out.append(fmt or "")

    return pd.DataFrame({"var": cols, "label": labels, "spss_type": types_out})

def show_value_table_json(
    df: pd.DataFrame,
    var: str,        # stripped name e.g., 'sexo'
    year: int,
    top: int | None = None,
    sort_index: bool = False,
    ascending: bool = True,
    basedir: Path = Path("Build/meta"),
) -> None:
    """
    Value counts (codes + labels) for df[var] restricted to df['ano']==year,
    using labels from the compact JSON (no big meta object needed).
    """
    if var not in df.columns:
        print(f"Variable '{var}' not found in df.")
        return

    vc = df.loc[pd.to_numeric(df["ano"], errors="coerce") == year, var]\
           .value_counts(dropna=False).rename("count").to_frame()

    mapping = labels_for(var, year, basedir=basedir)

    def to_label(code):
        if pd.isna(code):
            return None
        if isinstance(code, (int, np.integer)) or (isinstance(code, float) and code.is_integer()):
            return mapping.get(str(int(code)))
        return mapping.get(str(code))

    vc["label"] = [to_label(idx) for idx in vc.index]

    if sort_index:
        tmp = vc.reset_index()
        code_col = tmp.columns[0]
        tmp["__code_num"] = pd.to_numeric(tmp[code_col], errors="coerce")
        tmp = tmp.sort_values(
            by=["__code_num", code_col],
            ascending=ascending,
            na_position="last",
            kind="mergesort",
        ).drop(columns="__code_num")
        vc = tmp.set_index(code_col)

    if top:
        vc = vc.head(top)

    display(vc)
