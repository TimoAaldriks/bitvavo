from src.client import  get_bitvavo_client
from pprint import pprint
import time

if __name__ == "__main__":
    # Initialize the Bitvavo client
    client = get_bitvavo_client()

    # Example usage: Fetch and print the current markets
    for i in range(5):
      markets = client.markets({})
      time.sleep(0.1)
    # for market in markets:
    #     print(market["market"])