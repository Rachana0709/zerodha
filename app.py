import os
import time
import logging
import threading
from kiteconnect import KiteConnect, KiteTicker
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime

# --- Setup Logging ---
logging.basicConfig(
    filename="live_data_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
print("Log file created: live_data_debug.log")

# --- API Credentials ---
API_KEY = "u8lgeeb8v8fjdgzv"  # Replace with your API key
API_SECRET = "fvw1s3k7llr3z2bbv3xhrg0trfldk5hp"  # Replace with your API secret
ACCESS_TOKEN_FILE = "access_token.txt"

# Global Variables
NIFTY_INSTRUMENTS = []
instrument_token_to_row = {}
update_buffer = {}  # Buffer for batched updates
buffer_lock = threading.Lock()  # Lock for thread-safe operations

# --- Helper Functions ---
def get_access_token():
    """
    Generates or retrieves a valid access token.
    """
    kite = KiteConnect(api_key=API_KEY)

    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE, "r") as file:
            access_token = file.read().strip()
            logging.info("Access token loaded from file.")
            return kite, access_token

    print("Generating a new access token...")
    print("Login URL:", kite.login_url())
    request_token = input("Enter the request token from login URL: ").strip()
    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]

        with open(ACCESS_TOKEN_FILE, "w") as file:
            file.write(access_token)
        logging.info("New access token generated and saved.")
        return kite, access_token
    except Exception as e:
        logging.error(f"Error generating access token: {e}")
        raise e


def authenticate_google_sheets():
    """
    Authenticate and return the Google Sheets client.
    """
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("GOOGLE_SHEETS_CREDENTIALS", scope)
        return gspread.authorize(creds)
    except Exception as e:
        logging.error(f"Error authenticating Google Sheets: {e}")
        raise e


def fetch_nifty_instruments(kite):
    """
    Fetch instruments for the specified stocks.
    """
    global NIFTY_INSTRUMENTS
    target_symbols = {"INFY", "HCLTECH", "ITC", "RELIANCE", "SBICARD", "LT", "TCS", "ICICIBANK", "ASIANPAINT"}
    try:
        instruments = kite.instruments("NSE")
        NIFTY_INSTRUMENTS = [
            inst for inst in instruments if inst["tradingsymbol"] in target_symbols
        ]
        logging.info(f"Fetched selected instruments: {[inst['tradingsymbol'] for inst in NIFTY_INSTRUMENTS]}")
    except Exception as e:
        logging.error(f"Error fetching instruments: {e}")
        raise e


def setup_google_sheet():
    """
    Set up Google Sheets for storing live data.
    """
    global instrument_token_to_row
    try:
        client = authenticate_google_sheets()
        spreadsheet = client.open("Zerodha live data")
        logging.info(f"Spreadsheet '{spreadsheet.title}' opened successfully.")

        try:
            worksheet = spreadsheet.worksheet("Live Data")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="Live Data", rows="100", cols="11")
            worksheet.append_row(
                ["Instrument Token", "Trading Symbol", "Last Price", "Volume", "Open", "High", "Low", "Close", "Total Buy", "Total Sell", "Timestamp"]
            )
            logging.info("Worksheet 'Live Data' created and headers added.")

        existing_tokens = {int(cell) for cell in worksheet.col_values(1)[1:] if cell.isdigit()}
        row = 2
        for instrument in NIFTY_INSTRUMENTS:
            if instrument["instrument_token"] not in existing_tokens:
                worksheet.append_row([instrument["instrument_token"], instrument["tradingsymbol"], "", "", "", "", "", "", "", "", ""])
            instrument_token_to_row[instrument["instrument_token"]] = row
            row += 1

        logging.info(f"Initial rows for selected instruments set up: {instrument_token_to_row}")
        return worksheet
    except Exception as e:
        logging.error(f"Error in setup_google_sheet: {e}")
        raise e


# --- Batch Update Function ---
def batch_update_google_sheet():
    client = authenticate_google_sheets()
    spreadsheet = client.open("Zerodha live data")
    worksheet = spreadsheet.worksheet("Live Data")

    while True:
        time.sleep(30)  # Adjust interval as needed
        with buffer_lock:
            if update_buffer:  # Check if there is data in the buffer
                try:
                    for row, data in update_buffer.items():
                        worksheet.update(values=[data], range_name=f"A{row}:K{row}")
                    update_buffer.clear()  # Clear buffer after processing
                    logging.info("Batch update completed.")
                except Exception as e:
                    logging.error(f"Error during batch update: {e}")
            else:
                logging.info("No data to update, waiting for next cycle.")



def update_google_sheet_buffer(row, data):
    """
    Buffer updates instead of directly writing to Google Sheets.
    """
    with buffer_lock:
        update_buffer[row] = data


# --- WebSocket Event Handlers ---
def on_ticks(ws, ticks):
    logging.info(f"Received {len(ticks)} ticks.")
    try:
        for tick in ticks:
            instrument_token = tick["instrument_token"]
            if instrument_token in instrument_token_to_row:
                row = instrument_token_to_row[instrument_token]
                last_price = tick.get("last_price", "")
                volume = tick.get("volume_traded", 0)
                ohlc = tick.get("ohlc", {})
                total_buy = tick.get("total_buy_quantity", 0)
                total_sell = tick.get("total_sell_quantity", 0)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                data = [
                    instrument_token,
                    NIFTY_INSTRUMENTS[row - 2]["tradingsymbol"],
                    last_price,
                    volume,
                    ohlc.get("open", ""),
                    ohlc.get("high", ""),
                    ohlc.get("low", ""),
                    ohlc.get("close", ""),
                    total_buy,
                    total_sell,
                    timestamp,
                ]
                update_google_sheet_buffer(row, data)
                logging.info(f"Buffered row {row} for instrument {instrument_token}")
    except Exception as e:
        logging.error(f"Error in on_ticks: {e}")

def on_connect(ws, response):
    logging.info("WebSocket connected.")
    ws.subscribe([inst["instrument_token"] for inst in NIFTY_INSTRUMENTS])
    """
    Callback for WebSocket connection.
    """
    ws.subscribe([inst["instrument_token"] for inst in NIFTY_INSTRUMENTS])
    ws.set_mode(ws.MODE_FULL, [inst["instrument_token"] for inst in NIFTY_INSTRUMENTS])
    logging.info("WebSocket connected and subscribed to instruments.")



def on_close(ws, code, reason):
    logging.info(f"WebSocket closed: {code} - {reason}")
    """
    Callback for WebSocket disconnection.
    """
    logging.warning(f"WebSocket closed: {code} - {reason}")


# --- Main Function ---
def main():
    """
    Main script to start the live data collection and update process.
    """
    try:
        kite, access_token = get_access_token()
        fetch_nifty_instruments(kite)
        setup_google_sheet()

        # Start the batch update thread
        threading.Thread(target=batch_update_google_sheet, daemon=True).start()

        kws = KiteTicker(API_KEY, access_token)
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_close = on_close

        logging.info("Starting WebSocket...")
        kws.connect()
    except Exception as e:
        logging.error(f"Error in main script: {e}")
        raise


if __name__ == "__main__":
    main()
