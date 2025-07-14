import os, time, json, shutil
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pymysql
import time
from dotenv import load_dotenv

CONFIG_FILE = "config.json"
CODE_FILE = "Cleaned_Alpha3-M49_Code_Reference.csv"
DOWNLOAD_DIR = os.path.abspath("downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

load_dotenv(".env")

def log(msg): print(msg)
def log_ok(msg): print(f"\033[92m{msg}\033[0m")
def log_fail(msg): print(f"\033[91m{msg}\033[0m")

def load_alpha_map():
    df = pd.read_csv(CODE_FILE, sep=";", engine="python")
    df.columns = [c.strip() for c in df.columns]
    if "ISO-alpha3 Code" not in df.columns or "M49 Code" not in df.columns:
        log_fail("[ERROR] Required columns not found in the CSV file. Found columns: " + ", ".join(df.columns))
        exit(1)
    return {
        str(row["ISO-alpha3 Code"]).strip(): str(int(row["M49 Code"])).zfill(3)
        for _, row in df.iterrows()
        if pd.notnull(row.get("M49 Code"))
    }

def init_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    prefs = {"download.default_directory": DOWNLOAD_DIR}
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    return driver

def wait_for_download(expected_name, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        for f in os.listdir(DOWNLOAD_DIR):
            if f.endswith(".crdownload"):
                continue
            if f.lower().endswith(".xls"):
                full_path = os.path.join(DOWNLOAD_DIR, f)
                renamed = os.path.join(DOWNLOAD_DIR, expected_name)
                try:
                    shutil.move(full_path, renamed)
                    return renamed
                except Exception as e:
                    log_fail(f"[ERROR] Rename failed: {e}")
    return None

def safe_select_dropdown(driver, element_id, value, max_retries=3):
    """
    Try to select a dropdown value. If the value is already selected, skip selection.
    On connection error, restart driver and reload base URL if provided.
    Returns True if success or already selected, False if failure.
    """
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            dropdown = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, element_id))
            )
            select = Select(dropdown)
            
            # Check if value already selected
            selected_option = select.first_selected_option
            if selected_option.get_attribute("value") == value:
                log_ok(f"[OK] {element_id} already set to {value}, skipping selection")
                return True
            
            # Otherwise select the value
            select.select_by_value(value)
            # Wait for page reload/update - wait for dropdown to become stale then re-appear
            WebDriverWait(driver, 15).until(EC.staleness_of(dropdown))
            WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, element_id))
            )
            log_ok(f"[DONE] Selected {element_id} = {value}")
            return True
        except Exception as e:
            err_msg = str(e).lower()
            if ("connection refused" in err_msg) or ("no connection" in err_msg) or ("session" in err_msg):
                log_fail(f"[WARN] Connection error on attempt {attempt} for {element_id}={value}: {e}")
                return False
            else:
                log_fail(f"[ERROR] Failed to select {element_id}={value}: {e}")
                break
    return False

def scrape_country(driver, r_code, p_code, trade_type):
    log(f"[INFO] Scraping {trade_type} {r_code} {p_code}")

    filename = f"{r_code}_{p_code}_{trade_type}.html"
    expected_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(expected_path):
        log_ok(f"[OK] {filename} already exists, skipping download")
        return expected_path

    if not safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_Country", r_code, max_retries=3):
        return None

    if not safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_Partner", p_code, max_retries=3):
        return None

    trade_value = "E" if trade_type == "Export" else "I"
    if not safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_TradeType", trade_value, max_retries=3):
        return None

    # Set HS Code 4 digits, ignore failures
    try:
        safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_ProductClusterLevel", "4", max_retries=3)
    except: pass

    # Set Time Period 5 years, ignore failures
    try:
        safe_select_dropdown(driver, "ctl00_PageContent_GridViewPanelControl_DropDownList_NumTimePeriod", "5", max_retries=3)
    except: pass

    # Check if Export to Excel button exists before clicking
    try:
        export_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ctl00_PageContent_GridViewPanelControl_ImageButton_ExportExcel"))
        )
    except Exception:
        log_fail(f"[WARN] Export to Excel button not found for reporter {r_code}. Skipping this reporter.")
        return "NO_EXPORT_BUTTON"

    try:
        log(f"[INFO] Downloading XLS for {p_code} {trade_type}...")
        export_button.click()

        file_path = wait_for_download(filename)
        if file_path:
            new_path = file_path.replace(".xls", ".html")
            os.rename(file_path, new_path)
            log_ok(f"[DONE] Downloaded and renamed to {os.path.basename(new_path)}")
            return new_path
        else:
            log_fail("[ERROR] Download file not found after clicking export")
            return None
    except Exception as e:
        log_fail(f"[ERROR] Failed to download XLS: {e}")
        return None

def parse_html_to_sql(file_path, alpha_map, reporter, partner_code, status):
    try:
        log(f"[INFO] Parsing HTML: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        tables = soup.find_all("table")
        df = None

        log(f"[INFO] Found {len(tables)} table(s) in HTML")

        for idx, t in enumerate(tables):
            try:
                df_candidate = pd.read_html(StringIO(str(t)), header=None)[0]
                log(f"[INFO] Table {idx}: shape={df_candidate.shape}")
                if df_candidate.shape[0] < 2:
                    continue
                if df_candidate.iloc[0].astype(str).str.contains("Product code", case=False).any():
                    log_ok(f"[INFO] Valid table detected at index {idx}")
                    header_rows = df_candidate.iloc[:2]
                    headers = []
                    for col1, col2 in zip(header_rows.iloc[0], header_rows.iloc[1]):
                        h1 = str(col1).strip()
                        h2 = str(col2).strip()
                        headers.append(f"{h1} {h2}".strip())
                    df_candidate.columns = headers
                    df_candidate = df_candidate[2:].reset_index(drop=True)
                    df = df_candidate
                    break
            except Exception as e:
                log_fail(f"[ERROR] Failed to parse table {idx}: {e}")
                continue

        if df is None:
            log_fail(f"[ERROR] No valid data table found in {file_path}")
            return []

        log_ok(f"[INFO] Extracted dataframe with {len(df)} rows")

        df = df.fillna("")
        alpha3 = next((k for k, v in alpha_map.items() if v == partner_code), None)
        if not alpha3:
            log_fail(f"[WARN] Alpha-3 not found for {partner_code}, using numeric code")
            alpha3 = partner_code
            
        product_col = next((col for col in df.columns if "product code" in col.lower()), None)
        if not product_col:
            log_fail("[ERROR] HSCode column not found in HTML table")
            return []

        sql_lines = []
        for i, row in df.iterrows():
            hscode_raw = str(row.get(product_col, "")).strip()
            hscode_clean = hscode_raw.lstrip("'. ").strip()
            if not hscode_clean.isdigit():
                log(f"[WARN] Skipped non-numeric HSCode at row {i}: '{hscode_raw}' cleaned to '{hscode_clean}'")
                continue

            for col in df.columns:
                col_str = str(col).lower()
                if ("exports to" in col_str or "imports from" in col_str) and "value in" in col_str:
                    if "world" in col_str: # Ignore world
                        continue
                    year = "".join(filter(str.isdigit, col))
                    value = str(row.get(col, "")).replace(",", "").strip()
                    if value.lower() in ["", "null", "nan", "0"]:
                        continue
                    try:
                        nilai = float(value)
                        sql = (
                            f"('{reporter_alpha3}', NULL, NULL, '{alpha3}', NULL, NULL, "
                            f"NULL, {year}, '{hscode_clean}', NULL, 0, NULL, 0, {nilai}, '5', '{status}')"
                        )
                        sql_lines.append(sql)
                    except Exception as e:
                        log_fail(f"[ERROR] Failed to convert value at row {i}, column {col}: {e}")
                        continue

        log_ok(f"[DONE] Generated {len(sql_lines)} SQL line(s) from {file_path}")
        return sql_lines
    except Exception as e:
        log_fail(f"[ERROR] Failed to parse HTML: {e}")
        return []

def export_sql(all_sql_lines, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("SET NAMES utf8mb4;\nSET FOREIGN_KEY_CHECKS = 0;\n")
        f.write("INSERT INTO `tbtrade` (`Kode_Alpha3_Reporter`, `Provinsi_Reporter`, `Kota_Reporter`, "
                "`Kode_Alpha3_Partner`, `Provinsi_Partner`, `Kota_Partner`, `Bulan`, `Tahun`, `HSCode`, "
                "`ID_Sektor`, `Vol`, `Satuan`, `Tarif`, `Nilai`, `Kode_Sumber`, `Status`) VALUES\n")
        for i, line in enumerate(all_sql_lines):
            f.write(line + (",\n" if i < len(all_sql_lines)-1 else ";\n"))
        f.write("SET FOREIGN_KEY_CHECKS = 1;\n")
    log_ok(f"[SQL] Exported to {output_file}")

def insert_sql_to_database(sql_lines):
    cursor = None
    retry_count = 3
    for attempt in range(retry_count):
        try:
            conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_DATABASE"),
                charset='utf8mb4',
                connect_timeout=60  # Set timeout to 60 seconds
            )
            cursor = conn.cursor()
            query = (
                "INSERT INTO `{}` "
                "(`Kode_Alpha3_Reporter`, `Provinsi_Reporter`, `Kota_Reporter`, `Kode_Alpha3_Partner`, "
                "`Provinsi_Partner`, `Kota_Partner`, `Bulan`, `Tahun`, `HSCode`, `ID_Sektor`, `Vol`, "
                "`Satuan`, `Tarif`, `Nilai`, `Kode_Sumber`, `Status`) VALUES "
            ).format(os.getenv("DB_TABLE"))

            values = ",".join(sql_lines)
            full_query = query + values + ";"
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute(full_query)
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            conn.commit()
            log_ok(f"[DB] Inserted {len(sql_lines)} rows to database")
            break  # Exit retry loop if successful
        except Exception as e:
            log_fail(f"[DB ERROR] Attempt {attempt+1}/{retry_count}: {e}")
            if attempt < retry_count - 1:
                log("[INFO] Retrying...")
                time.sleep(5)  # Wait before retry
            else:
                log_fail(f"[DB ERROR] Failed after {retry_count} attempts.")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def restart_driver(driver, headless, base_url):
    log("[INFO] Restarting driver...")
    try:
        driver.quit()
    except Exception:
        log_fail("[ERROR] Failed to exit driver")
        pass

    try:
        new_driver = init_driver(headless)
        new_driver.get(base_url)
        log_ok("[OK] Driver started")
        return new_driver
    except Exception:
        log_fail("[ERROR] Failed to start driver")
        pass

    time.sleep(5)
    
# MAIN
with open(CONFIG_FILE) as f:
    config = json.load(f)
reporter_codes = config["reporter_codes"]
partner_codes = config["partner_codes"]
headless = config.get("headless", True)

alpha_map = load_alpha_map()
base_url = "https://www.trademap.org/Bilateral_TS.aspx"

for r_code in reporter_codes:
    all_sqls = []

    driver = init_driver(headless)
    log(f"[INFO] Opening base URL")
    driver.get(base_url)

    # Retry selecting reporter dropdown with driver refresh on error
    # if not safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_Country", r_code, max_retries=5):
    #     log_fail(f"[FATAL] Cannot select reporter {r_code} after multiple retries.")
    #     driver.quit()
    #     continue

    reporter_alpha3 = next((k for k, v in alpha_map.items() if v == r_code), r_code)

    skip_reporter = False  # Flag to skip entire reporter if export button missing
    for p_code in partner_codes:
        if skip_reporter:
            log_fail(f"[SKIP] Skipping reporter {r_code} due to missing export button.")
            break

        if p_code == r_code:
            continue
        for trade_type in config["type"]:
            MAX_RETRIES = 100
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    fpath = scrape_country(driver, r_code, p_code, trade_type)
                    if fpath == "NO_EXPORT_BUTTON":
                        # skip_reporter = True # mbuat skip kalo gk ada export button
                        continue
                        break
                    if fpath:
                        sqls = parse_html_to_sql(fpath, alpha_map, r_code, p_code, trade_type)
                        if sqls:
                            all_sqls.extend(sqls)
                        else:
                            log_fail(f"[SQL] No SQL generated from {fpath}")
                        break
                    else:
                        log_fail(f"[RETRY {attempt}] scrape_country failed for {r_code}-{p_code}-{trade_type}")

                        driver = restart_driver(driver, headless=True, base_url="https://www.trademap.org/Bilateral_TS.aspx")
                except Exception as e:
                    log_fail(f"[RETRY {attempt}/{MAX_RETRIES}] Failed for {r_code}-{p_code}-{trade_type}: {e}")

                    driver = restart_driver(driver, headless=True, base_url="https://www.trademap.org/Bilateral_TS.aspx")

                    # reselect reporter
                    if not safe_select_dropdown(driver, "ctl00_NavigationControl_DropDownList_Country", r_code, max_retries=5):
                        log_fail(f"[FATAL] Cannot select reporter {r_code} after multiple retries.")
                        driver.quit()
                        break
                    else:
                        log_fail(f"[FATAL] Giving up after {MAX_RETRIES} attempts.")

                    if attempt < MAX_RETRIES:
                        log("[INFO] Retrying after 5 seconds...")
                        time.sleep(5)
                    else:
                        log_fail(f"[FATAL] Giving up after {MAX_RETRIES} attempts.")
                        break

    if all_sqls:
        if config.get("export_to_sql"):
            export_sql(all_sqls, f"tbtrade_{r_code}_{trade_type}.sql")
        if config.get("insert_to_database"):
            insert_sql_to_database(all_sqls)
    else:
        log_fail(f"[WARN] No SQL lines to export for reporter {r_code}")

    driver.quit()