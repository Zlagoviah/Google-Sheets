import json
import os
import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

# 1) auth & open sheet
creds_json = os.environ["GOOGLE_CREDS_JSON"]  # secret from Actions [web:12]
gc = gspread.service_account_from_dict(json.loads(creds_json))  # dict-based auth [web:95][web:101]
ws = gc.open("Concentrador de ventas").worksheet("Cotizaciones")  # share with client_email [web:95]

df_desc = pd.read_csv('df_descc.csv', encoding='utf-8')

# Build batch_update requests
requests = []
sku_col_idx = list(df_desc.columns).index("sku") + 1  # SKU column (1-based)
col_M_idx = 13  # M is the 13th column

for idx, row in df_desc.iterrows():
    if row.get("similarity", 0) > .88 and pd.notnull(row.get("sku")):
        sheet_row = idx + 2  # 2: account for header (row 1)
        # SKU cell
        sku_cell = rowcol_to_a1(sheet_row, sku_col_idx)
        requests.append({"range": sku_cell, "values": [[row["sku"]]]})
        # Column M cell
        m_cell = rowcol_to_a1(sheet_row, col_M_idx)
        requests.append({"range": m_cell, "values": [[1]]})

# Perform batch update only if needed
if requests:
    ws.batch_update(requests)
