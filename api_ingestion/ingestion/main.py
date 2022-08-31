# %%
import datetime
import time

from ingestion_api import DaySummaryIngestor


if __name__ == "__main__":
    ingestor = DaySummaryIngestor(["ETH", "BTC", "LTC"])

    while True:
        ingestor.ingestion(datetime.date(2020,1,2))
        time.sleep(0.5)

# %%
