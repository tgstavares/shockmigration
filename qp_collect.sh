#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./qp_collect_spss.sh [RAW_DIR] [SPSS_DIR]
# Defaults:
RAW_DIR="${1:-Raw}"
SPSS_DIR="${2:-Spss}"

echo "RAW_DIR:  $RAW_DIR"
echo "SPSS_DIR: $SPSS_DIR"
mkdir -p "$SPSS_DIR"

# Helper: map 2-digit YY -> 4-digit YYYY for QP years (1983–1999, 2000–2023).
to_yyyy() {
  yy="$1"
  yy_num=$((10#$yy))  # avoid octal with leading zeros
  if   [ "$yy_num" -ge 83 ] && [ "$yy_num" -le 99 ]; then echo $((1900 + yy_num))
  elif [ "$yy_num" -ge 0  ] && [ "$yy_num" -le 23 ];  then echo $((2000 + yy_num))
  else  echo $((1900 + yy_num))  # fallback (shouldn't happen for your range)
  fi
}

# 1) Unzip all .zip files in Raw/ (top-level)
if find "$RAW_DIR" -maxdepth 1 -type f -name '*.zip' | grep -q .; then
  echo "Unzipping .zip files in $RAW_DIR ..."
  find "$RAW_DIR" -maxdepth 1 -type f -name '*.zip' -print0 \
  | while IFS= read -r -d '' zipfile; do
      echo "  - $(basename "$zipfile")"
      ditto -x -k "$zipfile" "$RAW_DIR"
    done
else
  echo "No .zip files to unzip in $RAW_DIR."
fi

# 2)+3a) Find SPSS files like QP_Trabalhadores_YYYY*.sav, rename -> QP_Trabalhadores_YYYY.sav, move to Spss/
echo "Collecting and renaming SPSS files ..."
find "$RAW_DIR" -type f -iname 'QP_Trabalhadores_*.sav' -print0 \
| while IFS= read -r -d '' f; do
    base="${f##*/}"
    # Extract first 4-digit year after the fixed prefix
    year="$(printf '%s' "$base" | sed -n 's/^QP_Trabalhadores_\([0-9][0-9][0-9][0-9]\).*/\1/p')"
    if [ -n "$year" ]; then
      dest="$SPSS_DIR/QP_Trabalhadores_${year}.sav"
      if [ -e "$dest" ]; then
        echo "  ! Skipping (already exists): $dest   [source: $base]"
      else
        echo "  ✓ Moving $base → $(basename "$dest")"
        mv "$f" "$dest"
      fi
    else
      echo "  ! Skipping (no year match): $base"
    fi
  done


# 3b) Empresas: *Emp_YY*.sav, *Emp_YYYY*.sav, *Empresas_YYYY*.sav -> QP_Empresas_YYYY.sav
echo "Collecting and renaming SPSS files (Empresas) ..."
find "$RAW_DIR" -type f \( -iname '*Emp_[0-9]*.sav' -o -iname '*Empresas_*.sav' \) -print0 \
| while IFS= read -r -d '' f; do
  base="${f##*/}"

  # Prefer 4-digit year immediately after Emp_/Empresas_
  year=$(printf '%s' "$base" | LC_ALL=C sed -nE 's/.*([Ee][Mm][Pp]([Rr][Ee][Ss][Aa][Ss])?)_([0-9]{4}).*/\3/p')
  if [ -z "$year" ]; then
    # Fallback: two-digit year after Emp_/Empresas_
    yy=$(printf '%s' "$base" | LC_ALL=C sed -nE 's/.*([Ee][Mm][Pp]([Rr][Ee][Ss][Aa][Ss])?)_([0-9]{2}).*/\3/p')
    [ -n "$yy" ] && year="$(to_yyyy "$yy")"
  fi

  if [ -n "$year" ]; then
    dest="$SPSS_DIR/QP_Empresas_${year}.sav"
    if [ -e "$dest" ]; then
      echo "  ! Skipping (already exists): $dest   [source: $base]"
    else
      echo "  ✓ Moving $base → $(basename "$dest")"
      mv "$f" "$dest"
    fi
  else
    echo "  ! Skipping (no Empresas year match): $base"
  fi
done

# 3c) Estabelecimentos: *Est_YY*.sav, *Est_YYYY*.sav, *Estabelecimentos_YYYY*.sav -> QP_Estabelecimentos_YYYY.sav
echo "Collecting and renaming SPSS files (Estabelecimentos) ..."
find "$RAW_DIR" -type f \( -iname '*Est_[0-9]*.sav' -o -iname '*Estabelec*.sav' \) -print0 \
| while IFS= read -r -d '' f; do
  base="${f##*/}"

  # Prefer 4-digit year immediately after Est_/Estabelecimentos_
  year=$(printf '%s' "$base" | LC_ALL=C sed -nE 's/.*([Ee][Ss][Tt]([Aa][Bb][Ee][Ll][Ee][Cc][Ii][Mm][Ee][Nn][Tt][Oo][Ss])?)_([0-9]{4}).*/\3/p')
  if [ -z "$year" ]; then
    # Fallback: two-digit year after Est_/Estabelecimentos_
    yy=$(printf '%s' "$base" | LC_ALL=C sed -nE 's/.*([Ee][Ss][Tt]([Aa][Bb][Ee][Ll][Ee][Cc][Ii][Mm][Ee][Nn][Tt][Oo][Ss])?)_([0-9]{2}).*/\3/p')
    [ -n "$yy" ] && year="$(to_yyyy "$yy")"
  fi

  if [ -n "$year" ]; then
    dest="$SPSS_DIR/QP_Estabelecimentos_${year}.sav"
    if [ -e "$dest" ]; then
      echo "  ! Skipping (already exists): $dest   [source: $base]"
    else
      echo "  ✓ Moving $base → $(basename "$dest")"
      mv "$f" "$dest"
    fi
  else
    echo "  ! Skipping (no Estabelecimentos year match): $base"
  fi
done

# 4) Remove any remaining .sav files from Raw/ (recursively)
echo "Removing remaining .sav files from $RAW_DIR ..."
find "$RAW_DIR" -type f -iname '*.sav' -exec rm -f {} +

echo "Done."
