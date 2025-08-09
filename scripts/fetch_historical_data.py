import sys
import os
import time
from datetime import datetime, timedelta

# Add the project root to the Python path to allow imports from 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.client import get_bitvavo_client
from src.database import setup_database, insert_candles, get_latest_candle_timestamp
from src.logger import logger
from src.config import START_DATE


def backfill_all_markets(interval='1h', limit=1000):
    """
    Fetches the latest candle data for all available markets on Bitvavo
    and stores it in the database.
    """
    logger.info(f"--- Starting Data Ingestion for interval: {interval} ---")
    setup_database()

    client = get_bitvavo_client()

    # Get all available markets
    try:
        response = client.markets({})
    except Exception as e:
        logger.error(f"Failed to fetch market list from Bitvavo: {e}")
        return

    if not response:
        logger.warning("Could not fetch market list. Exiting.")
        return

    # Filter for only currently trading markets
    markets = [m['market'] for m in response if m['status'] == 'trading']
    logger.info(f"Found {len(markets)} active markets to process.")

    for i, market in enumerate(markets):
        try:
            logger.info(f"[{i+1}/{len(markets)}] Fetching latest {limit} candles for {market}...")
            candles = client.candles(market, interval, {'limit': limit})
            if candles:
                insert_candles(market, candles)
            else:
                logger.info(f"  -> No candle data returned for {market}.")
            # Be respectful of API rate limits by adding a small delay
            time.sleep(0.2)
        except Exception as e:
            logger.error(f"An error occurred while fetching data for {market}: {e}")
            continue # Move to the next market

    logger.info(f"--- Data Ingestion for interval {interval} Complete ---")
    

def fetch_historical_data(start_date=datetime.strptime(START_DATE, "%Y-%m-%d"), end_date=None):
    """
    Fetches historical candle data for all markets from start_date to end_date.
    If end_date is not provided, it defaults to the current date.
    """

    if end_date is None:
        end_date = datetime.now()

    logger.info(f"Fetching historical data from {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}...")
    client = get_bitvavo_client()

    setup_database()

    # Get all available markets
    try:
        response = client.markets({})
    except Exception as e:
        logger.error(f"Failed to fetch market list from Bitvavo: {e}")
        return

    if not response:
        logger.warning("Could not fetch market list. Exiting.")
        return

    markets = [m['market'] for m in response if m['status'] == 'trading']
    logger.info(f"Found {len(markets)} active markets to process.")

    for market in markets:
        # We need to fetch data in chunks, so we will loop through the date range
        # We start from the most recent date and go backwards
        fetch_date = end_date
        latest_candle_ts = get_latest_candle_timestamp(market)

        if latest_candle_ts:
            # Convert the latest candle timestamp to a datetime object
            start_date = datetime.fromtimestamp(latest_candle_ts / 1000)

        while fetch_date > start_date:
            candles = client.candles(market, '1h', end=fetch_date)
    
            if candles:
                # Get the date of the last candle to continue fetching
                fetch_date = datetime.fromtimestamp(candles[-1][0] / 1000)
                candles = [c for c in candles if c[0] > latest_candle_ts]  # Filter out already existing candles

                if not candles:
                    logger.info(f"No new candles to insert for {market} after {fetch_date:%Y-%m-%d}.")
                    break

                insert_candles(market, candles)
                logger.info(f"Inserted {len(candles)} candles for {market}. {datetime.fromtimestamp(candles[-1][0] / 1000):%Y-%m-%d %H:%M} -> {datetime.fromtimestamp(candles[0][0] / 1000):%Y-%m-%d %H:%M}")

            else:
                logger.info(f"No more data available for {market} before {fetch_date:%Y-%m-%d}.")
                break  # No more data available for this market


            time.sleep(0.1)  # Respect API rate limits


if __name__ == "__main__":
    fetch_historical_data()
    # backfill_all_markets(interval='1h')
    # You could also fetch other intervals
    # backfill_all_markets(interval='4h')