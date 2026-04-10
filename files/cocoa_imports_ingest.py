"""
Hardmine — TDM Cocoa Imports Ingest (Major Importers)
======================================================
Usage:
    python cocoa_imports_ingest.py            # incremental
    python cocoa_imports_ingest.py --full     # full history from 201501

NOTE: Uses username/password auth.
Saves to: .../Cocoa Flows/files/data/tdm_cocoa_imports.parquet
"""

import argparse
import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import country_converter as coco
import numpy as np
import pandas as pd
import requests

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "cocoa_imports_ingest.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

USERNAME = "etg.cc"
PASSWORD = "etg.nl"
BASE_URL = "https://www1.tdmlogin.com/tdm/api/api.asp"

REPORTERS = "NL,MY,US,BE,DEC,ID,CA,FRC,TR,ESC,ITC,SG,UKS,CH,JP,BR,BG"
HS_CODES  = ["180100", "180310", "180320", "180400", "180500"]

FLOW         = "I"
LEVEL        = "6"
FREQUENCY    = "M"
SEPARATOR    = "T"
AGG_PARTNERS = "Y"
CONV         = "1"

PERIOD_FULL_BEGIN = "201501"
PERIOD_END        = "203012"

OUT_FILE   = Path(__file__).parent / "data" / "tdm_cocoa_imports.parquet"
COLUMNS    = ["REPORTER", "PARTNER", "COMMODITY", "YEAR", "MONTH", "QTY1"]
DEDUP_KEYS = ["REPORTER", "PARTNER", "COMMODITY", "YEAR", "MONTH"]

COMMODITY_TAG = {180100:"Beans", 180310:"Liquor", 180320:"Paste", 180400:"Butter", 180500:"Powder"}
BEQ_MULTIPLIER = {180100:1.00, 180310:1.22, 180320:0.00, 180400:2.70, 180500:0.00}

REPORTER_REGION = {
    "Belgium":"Europe","Netherlands":"Europe","Germany Customs":"Europe",
    "France Customs":"Europe","Italy ISTAT":"Europe","Spain Customs":"Europe",
    "Switzerland":"Europe","United Kingdom HMRC":"Europe","Bulgaria":"Europe",
    "United States":"NAM","Canada":"NAM",
    "Japan":"Asia","Indonesia":"Asia","Malaysia":"Asia","Singapore":"Asia",
    "Brazil":"LATAM","Turkey":"Other",
}

PARTNER_FIX = {
    "United States":"United States of America","Russia":"Russian Federation",
    "South Korea":"Korea, Republic of","Iran":"Iran, Islamic Republic of",
    "Vietnam":"Viet Nam","Venezuela":"Venezuela, Bolivarian Republic of",
    "Bolivia":"Bolivia, Plurinational State of","Tanzania":"Tanzania, United Republic of",
    "Taiwan":"Taiwan, Province of China","Cote d'Ivoire":"Cote d'Ivoire",
    "Congo (ROC)":"Congo","Congo (DROC)":"Democratic Republic of the Congo",
    "Netherlands Antilles":"Other","Duty Free Shops":"Other",
    "Stores and Provisions":"Other","Other Asia, nes":"Other",
    "Free Zones":"Other","High Seas":"Other","Unidentified":"Other",
}

NAM = {"Canada","United States of America","Bermuda","Greenland"}
LATAM = {
    "Argentina","Belize","Bolivia, Plurinational State of","Brazil","Chile",
    "Colombia","Costa Rica","Cuba","Dominican Republic","Ecuador","El Salvador",
    "French Guiana","Guatemala","Guyana","Haiti","Honduras","Jamaica","Mexico",
    "Nicaragua","Panama","Paraguay","Peru","Suriname","Trinidad and Tobago",
    "Uruguay","Venezuela, Bolivarian Republic of",
}


def build_url(period_begin):
    return (
        f"{BASE_URL}?username={USERNAME}&password={PASSWORD}"
        f"&flow={FLOW}&reporter={REPORTERS}&partners=all"
        f"&periodBegin={period_begin}&periodEnd={PERIOD_END}"
        f"&hsCode={','.join(HS_CODES)}&levelDetail={LEVEL}"
        f"&frequency={FREQUENCY}&separator={SEPARATOR}"
        f"&aggregatePartners={AGG_PARTNERS}&conv={CONV}"
    )


def fetch_tdm(period_begin):
    resp = requests.get(build_url(period_begin), timeout=120)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-16")), sep="\t", low_memory=False)
    return df[COLUMNS].copy()


def add_derived_columns(df):
    df["CROP_YEAR"] = (
        ((df["YEAR"] - (df["MONTH"] < 10).astype(int)) % 100).astype(str).str.zfill(2)
        + "/" + ((df["YEAR"] + (df["MONTH"] >= 10).astype(int)) % 100).astype(str).str.zfill(2)
    )
    df["CROP_MONTH_NUM"] = ((df["MONTH"] + 2) % 12) + 1
    p = df["PARTNER"].astype(str).str.strip().replace(PARTNER_FIX)
    cc = coco.CountryConverter()
    logging.getLogger("country_converter").setLevel(logging.ERROR)
    continent = pd.Series(cc.convert(names=p.tolist(), to="continent", not_found="Other"), index=df.index)
    df["REGION"] = np.select(
        [p.eq("Other"), p.isin(NAM), p.isin(LATAM),
         continent.eq("Europe"), continent.eq("Asia"),
         continent.eq("Africa"), continent.eq("Oceania")],
        ["Other","NAM","LATAM","Europe","Asia","Africa","Oceania"], default="Other",
    )
    df["REPORTER_REGION"] = df["REPORTER"].map(REPORTER_REGION).fillna("Other")
    df["COMMODITY_TAG"]   = df["COMMODITY"].map(COMMODITY_TAG)
    df["BEQ"]             = df["QTY1"] * df["COMMODITY"].map(BEQ_MULTIPLIER)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Cocoa Imports Ingest  |  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if args.full or not OUT_FILE.exists():
        period_begin = PERIOD_FULL_BEGIN
    else:
        period_begin = f"{int(pd.read_parquet(OUT_FILE, columns=['YEAR'])['YEAR'].max())}01"

    new_data = add_derived_columns(fetch_tdm(period_begin))

    if OUT_FILE.exists() and not args.full:
        merged = pd.concat([pd.read_parquet(OUT_FILE), new_data], ignore_index=True)
        df = merged.drop_duplicates(subset=DEDUP_KEYS, keep="last")
    else:
        df = new_data.copy()

    df.to_parquet(OUT_FILE, engine="pyarrow", index=False)
    log.info("Saved -> %s  |  %d rows", OUT_FILE, len(df))


if __name__ == "__main__":
    main()
