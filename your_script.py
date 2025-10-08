from gspread.utils import rowcol_to_a1

import math
import os, json
import gspread
import pandas as pd
import re
from datetime import datetime

  # 1) auth & open sheet
creds_json = os.environ["GOOGLE_CREDS_JSON"]  # secret from Actions [web:12]
gc = gspread.service_account_from_dict(json.loads(creds_json))  # dict-based auth [web:95][web:101]
ws = gc.open("Concentrador de ventas").worksheet("Cotizaciones")  # share with client_email [web:95]

df_desc = pd.read_csv('df_descc.csv', encoding='utf-8')
df_desc['similarity'] = df_desc['similarity'].round(2)

# Build batch_update requests
# requests = []
# sku_col_idx = list(df_desc.columns).index("sku") + 1  # SKU column (1-based)
# col_M_idx = 13  # M is the 13th column
# col_cat_idx = 14

# for idx, row in df_desc.iterrows():
#     if row.get("similarity", 0) >= .88 and pd.notnull(row.get("sku")):
#         sheet_row = idx + 2  # 2: account for header (row 1)
#         # SKU cell
#         sku_cell = rowcol_to_a1(sheet_row, sku_col_idx)
#         requests.append({"range": sku_cell, "values": [[row["sku"]]]})
#         # Column M cell
#         m_cell = rowcol_to_a1(sheet_row, col_M_idx)
#         requests.append({"range": m_cell, "values": [[row["similarity"]]]})
#         # Column N cell
#         if [[row["category_homol"]]]:
#           n_cell = rowcol_to_a1(sheet_row, col_cat_idx)
#           requests.append({"range": n_cell, "values": [[row["category_homol"]]]})

def build_sheet_updates(df_desc, sku_col_idx, col_M_idx, col_cat_idx, similarity_threshold=0.88, round_similarity=None):
    """
    Return a list of value ranges for Google Sheets from df_desc.
    - df_desc: DataFrame must have columns 'sku', 'similarity', 'category_homol'
    - sku_col_idx, col_M_idx, col_cat_idx: 1-based column indices (used by rowcol_to_a1)
    - similarity_threshold: numeric threshold to select rows
    - round_similarity: if int, round similarity to this many decimals before sending
    """
    requests = []

    # defensive: ensure required columns exist
    for c in ("sku", "similarity", "category_homol"):
        if c not in df_desc.columns:
            raise KeyError(f"Missing required column: {c}")

    # create mask: similarity >= threshold AND sku not null/empty
    # coerce similarity to numeric safely (non-convertible -> NaN)
    similarity = pd.to_numeric(df_desc["similarity"], errors="coerce")
    mask = (similarity >= similarity_threshold) & df_desc["sku"].notna() & (df_desc["sku"].astype(str).str.strip() != "")

    if not mask.any():
        return requests  # nothing to do

    # Only iterate over candidate rows
    candidates = df_desc.loc[mask].copy()
    # ensure similarity column in candidates is numeric and optionally rounded
    candidates["similarity"] = pd.to_numeric(candidates["similarity"], errors="coerce").astype(float)
    if isinstance(round_similarity, int):
        candidates["similarity"] = candidates["similarity"].round(round_similarity)

    # Using itertuples is faster than iterrows
    for idx, row in candidates.reset_index().itertuples(index=False, name=None):
        # after reset_index, tuple layout is (original_index, col1, col2, ...)
        # but to avoid confusion, we built candidates.reset_index() and then use the DataFrame .itertuples below instead:
        pass

    # Better: iterate with itertuples on candidates (preserve original index)
    for r in candidates.itertuples(index=True, name="Row"):
        sheet_row = r.Index + 2  # +2 for header row offset
        # SKU cell
        sku_cell = rowcol_to_a1(sheet_row, sku_col_idx)
        requests.append({"range": sku_cell, "values": [[r.sku]]})

        # Column M cell (similarity)
        m_cell = rowcol_to_a1(sheet_row, col_M_idx)
        # Make sure similarity is a native python float/str (no numpy scalar)
        sim_val = float(r.similarity) if (r.similarity is not None and not (isinstance(r.similarity, float) and math.isnan(r.similarity))) else None
        requests.append({"range": m_cell, "values": [[sim_val]]})

        # Column N cell (category_homol) only if non-null and non-empty string
        cat = r.category_homol
        if pd.notna(cat) and str(cat).strip() != "":
            n_cell = rowcol_to_a1(sheet_row, col_cat_idx)
            requests.append({"range": n_cell, "values": [[cat]]})

    return requests
  
# e.g., df_desc is already loaded
updates = build_sheet_updates(df_desc, sku_col_idx=2, col_M_idx=13, col_cat_idx=14, similarity_threshold=0.88, round_similarity=3)

# If you need to split into chunks of 500 updates (Google Sheets has limits):
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

for chunk in chunked(updates, 500):
    body = {"valueInputOption": "USER_ENTERED", "data": chunk}

# Perform batch update only if needed
if requests:
  ws.batch_update(requests)

# create a timestamp like 081020251530 (8 Oct 2025, 15:30)
timestamp = datetime.now().strftime("%d%m%Y%H%M")
filename = f"log{timestamp}.csv"

# example: convert updates list to DataFrame for logging
if updates:
    log_df = pd.DataFrame(updates)
    log_df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Saved log file: {filename}")
