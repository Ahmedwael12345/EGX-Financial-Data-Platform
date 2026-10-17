# src/egx_scraper.py
import os
import uuid
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import pandas as pd
from curl_cffi import requests
from tqdm import tqdm

# 1. Setup Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_CSV_PATH = BRONZE_DIR / "egx_raw_latest.csv"

COMPANIES = [
    "ETEL", "SWDY", "TMGH", "EGAL", "MFPC", "ABUK", "ALCN", "ORAS",
    "EAST", "EFIH", "EMFD", "FWRY", "SCTS", "ORHD", "GPPL", "VLMR",
    "VLMRA", "EFID", "JUFO", "PHDC", "HRHO", "OCDI", "GBCO", "FERC",
    "BTFH", "HELI", "IRON", "RAYA", "BIOC", "ARCC", "CCAP", "EGCH",
    "TAQA", "CIRA", "VALU", "SCEM", "CLHO", "MBSC", "MCQE", "PHAR",
    "SKPC", "MTIE", "ORWE", "POUL", "AMOC", "EGTS", "EGSA", "MOIL",
    "EFIC", "MASR", "NIPH", "TALM", "KORA", "ATQA", "MHOT", "BINV",
    "CICH", "ISPH", "AMES", "RMDA", "CSAG", "NAPR", "OIH", "AMIA",
    "IFAP", "MIPH", "MOIN", "MPRC", "CPCI", "OLFI", "EGAS", "ISMQ",
    "MPCI", "PHTV", "SUGR", "PRDC", "BONY", "AXPH", "ZMID", "DOMT",
    "GOUR", "ELEC", "SPHT", "ARAB", "SPIN", "ACAP", "NINH", "OCPH",
    "ENGC", "MCRO", "AFMC", "SVCE", "MICH", "KABO", "CNFN", "GSSC",
    "WCDF", "DSCW", "SDTI", "MFSC", "AMER", "OFH", "ACGC", "UNIT",
    "AJWA", "KZPC", "GDWA", "CRST", "UEFM", "ADCI", "GPIM", "ACTF",
    "ELKA", "ASCM", "ELSH", "CFGH", "ICFC", "LCSW", "ZEOT", "NAHO",
    "ALRA", "ATLC", "INFI", "ACAMD", "ETRS", "MPCO", "EDFM", "GTWL",
    "ISMA", "DAPH", "MAAL", "NARE", "SMFR", "CEFM", "PHGC", "MILS",
    "SNFC", "IDRE", "GGRN", "NCCW", "RACC", "ICID", "EALR", "ADPC",
    "MOSC", "AALR", "EHDR", "KRDI", "WKOL", "DTPP", "ECAP", "MENA",
    "CERA", "GGCC", "ODIN", "MBEG", "SCFM", "CAED", "ASPI", "DEIN",
    "SIPC", "PRCL", "AIHC", "LUTS", "NDRL", "SEIG", "SEIGA", "NHPS",
    "IEEC", "UEGC", "OBRI", "MEPA", "AIDC", "ALUM", "COSG", "RUBX",
    "POCO", "RREI", "TANM", "GTEX", "EBSC", "AFDI", "RTVC", "AMII",
    "TWSA", "UNIP", "APSW", "PRMH", "ICLE", "MEGM", "EASB", "RAKT",
    "MOED", "ROTO", "TYCN", "SPMD", "KWIN", "EEII", "AREH", "CCRS",
    "FCMD", "EPCO", "GRCA", "GIHD", "TRTO", "ELWA", "ELNA", "MMAT",
    "DGTZ", "DCCC", "NEDA", "EPPK", "GMCI", "EOSB", "CPME", "COPR"
]

STATEMENTS = {
    "income-statement": 1,
    "balance-sheet": 3,
    "cash-flow-statement": 3,
}

def save_bronze_incremental(new_scraped_df, output_path):
    """Merges new run data into Bronze CSV incrementally."""
    business_keys = ["Company", "Statement Type", "Financial Term", "End Of Quarter"]

    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        combined_df = pd.concat([existing_df, new_scraped_df], ignore_index=True)
        deduped_df = combined_df.drop_duplicates(subset=business_keys, keep="last")
        print(f"🧹 Filtered {len(combined_df) - len(deduped_df)} duplicate records.")
    else:
        deduped_df = new_scraped_df.drop_duplicates(subset=business_keys, keep="last")

    deduped_df.to_csv(output_path, index=False)
    print(f"ronze Incremental Load Complete ({len(deduped_df)} records) -> {output_path}")

def main():
    all_records = []
    run_id = str(uuid.uuid4())
    extracted_at = datetime.now(timezone.utc).isoformat()

    session = requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://stockanalysis.com/",
    })

    print("🚀 Starting StockAnalysis Incremental Bronze Scraper...\n")
    company_pbar = tqdm(COMPANIES, desc="Overall Progress", unit="company")

    for company in company_pbar:
        for statement, n_tables in STATEMENTS.items():
            company_pbar.set_postfix_str(f"Scraping: {company} | {statement}")
            url = f"https://stockanalysis.com/quote/egx/{company}/financials/{statement}/?p=quarterly"

            try:
                resp = session.get(url, timeout=15)
                if resp.status_code != 200:
                    continue

                tables = pd.read_html(StringIO(resp.text))
                if not tables:
                    continue

                for df_table in tables[:n_tables]:
                    if df_table.empty:
                        continue

                    if isinstance(df_table.columns, pd.MultiIndex):
                        df_table.columns = df_table.columns.get_level_values(-1)

                    first_col = df_table.columns[0]
                    df_table = df_table.rename(columns={first_col: "Financial Term"})

                    df_table = df_table.melt(
                        id_vars=["Financial Term"],
                        var_name="End Of Quarter",
                        value_name="Amount",
                    )

                    df_table["Company"] = company
                    df_table["Statement Type"] = statement
                    all_records.append(df_table)

            except Exception as e:
                continue

    if all_records:
        df_new = pd.concat(all_records, ignore_index=True)
        df_new["Extracted_At_UTC"] = extracted_at
        df_new["Run_ID"] = run_id
        save_bronze_incremental(df_new, str(BRONZE_CSV_PATH))
    else:
        print("\No data was extracted in this run.")

if __name__ == "__main__":
    main()