import os, json
import gspread
import pandas as pd

creds_json = os.environ["GOOGLE_CREDS_JSON"]  # secret from Actions [web:12]
gc = gspread.service_account_from_dict(json.loads(creds_json))  # dict-based auth [web:95][web:101]
ws = gc.open("Concentrador de ventas").worksheet("Cotizaciones")  # share with client_email [web:95]
df = pd.DataFrame(ws.get_all_records())  # rows to DataFrame [web:95]

df.to_csv('df_desc.csv', index=False)
