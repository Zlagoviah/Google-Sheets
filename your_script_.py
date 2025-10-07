import os, json
import gspread
import pandas as pd
import re

from datetime import datetime

DATE_COL = "fecha"

# ─── 1. LOAD YOUR DATAFRAME ────────────────────────────────────────────────────
def normalize(col: str) -> str:
  # strip, turn spaces and dashes into underscores, lowercase
  return re.sub(r"[\s\-]+", "_", col.strip()).lower()

def load_my_df() -> pd.DataFrame:
  """Fetches the sheet, renames columns, parses types."""
  # 1) auth & open sheet
  creds_json = os.environ["GOOGLE_CREDS_JSON"]  # secret from Actions [web:12]
  gc = gspread.service_account_from_dict(json.loads(creds_json))  # dict-based auth [web:95][web:101]
  ws = gc.open("Concentrador de ventas").worksheet("Cotizaciones")  # share with client_email [web:95]
  
  # 2) load all records
  raw = pd.DataFrame(ws.get_all_records())
  
  # Normalize column names
  raw.columns = [normalize(c) for c in raw.columns]
  
  # Debug to see normalized names
  print("Normalized Columns:", raw.columns.tolist())
  
  mapping = {
      "id":                               "id",
      "path":                             "path",
      "no._folio_de_cotizacion":          "no_cotizacion",
      "fecha":                            "fecha",
      "producto":                         "producto",
      "sku":                              "sku",
      "descripción":                      "descripcion",
      "unidades":                         "unidades",
      "precio_p._(precio_sin_iva)":       "precio_p",
      "precio_p/unidad_(precio_sin_iva)": "precio_unitario",
      "precio_c/u":                       "precio_cu",
      "precio_final":                     "precio_final"
  }
  
  # 3) rename columns: map your sheet headers → model fields
  df = raw.rename(columns=mapping)
  
  # 4) Ensure all required columns are present
  required = set(mapping.values())
  missing = required - set(df.columns)
  if missing:
      raise KeyError(f"Missing expected columns after rename: {missing}")
  
  # 5) parse & cast types **vectorized**
  df[DATE_COL] = pd.to_datetime(
      df[DATE_COL], format="%d/%m/%Y %H:%M:%S", dayfirst=True
  ).dt.date
  
  df["unidades"]          = pd.to_numeric(df["unidades"],  errors="coerce").fillna(0).astype(int)
  df["precio_p"]          = pd.to_numeric(df["precio_p"].str.replace(r'[$,]', '', regex=True), errors="coerce").fillna(0.0).astype(float)
  df["precio_unitario"]   = pd.to_numeric(df["precio_unitario"].str.replace(r'[$,]', '', regex=True), errors="coerce").fillna(0.0).astype(float)
  df["precio_cu"]         = pd.to_numeric(df["precio_cu"].str.replace(r'[$,]', '', regex=True), errors="coerce").fillna(0.0).astype(float)
  df["precio_final"]      = pd.to_numeric(df["precio_final"].str.replace(r'[$,]', '', regex=True), errors="coerce").fillna(0.0).astype(float)
  
  # 1a. Identify all columns you want to trim (e.g. object‑dtype columns)
  str_cols = df.select_dtypes(include=['object']).columns
  
  # 2a. On each of those columns, map only the str values through .strip()
  df[str_cols] = df[str_cols].apply(
      lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)
  )
  
  # 6) Drop any fully empty rows/columns and return
  return df.dropna(how="all", axis=1).dropna(how="all", axis=0)

df = load_my_df()
df.to_csv('df_desc.csv', index=False)
