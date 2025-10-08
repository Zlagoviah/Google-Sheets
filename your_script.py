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
requests = []
sku_col_idx = list(df_desc.columns).index("sku") + 1  # SKU column (1-based)
col_M_idx = 13  # M is the 13th column
col_cat_idx = 14

for idx, row in df_desc.iterrows():
    if row.get("similarity", 0) >= .88 and pd.notnull(row.get("sku")):
        sheet_row = idx + 2  # 2: account for header (row 1)
        # SKU cell
        sku_cell = rowcol_to_a1(sheet_row, sku_col_idx)
        requests.append({"range": sku_cell, "values": [[row["sku"]]]})
        # Column M cell
        m_cell = rowcol_to_a1(sheet_row, col_M_idx)
        requests.append({"range": m_cell, "values": [[row["similarity"]]]})
        # Column N cell (category_homol) — only if not empty
        category = row.get("category_homol")
        corrected = row.get("category_corrected")
        if pd.notna(category) and str(category).strip() != "" and str(corrected).strip() == "True":
            n_cell = rowcol_to_a1(sheet_row, col_cat_idx)
            requests.append({"range": n_cell, "values": [[category]]})

# If you need to split into chunks of 500 updates (Google Sheets has limits):
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

for chunk in chunked(requests, 500):
    body = {"valueInputOption": "USER_ENTERED", "data": chunk}

# Perform batch update only if needed
if requests:
  ws.batch_update(requests)

# create a timestamp like 081020251530 (8 Oct 2025, 15:30)
timestamp = datetime.now().strftime("%d%m%Y%H%M")
filename = f"log{timestamp}.csv"

# example: convert updates list to DataFrame for logging
if requests:
    log_df = pd.DataFrame(requests)
    log_df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Saved log file: {filename}")
