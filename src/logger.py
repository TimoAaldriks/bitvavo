import logging
import os
import sys

# --- Logger Setup ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Create a logger
logger = logging.getLogger("trading_bot")
logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all messages

# Create handlers for console and file output
console_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler(os.path.join(LOG_DIR, "data_ingestion.log"))

# Set levels for handlers: INFO for console, DEBUG for file
console_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.DEBUG)

# Create a standard log format
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(log_format)
file_handler.setFormatter(log_format)

# Add handlers to the logger, but only if they haven't been added before
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)