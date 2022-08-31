import logging
from typing import Union

import requests as req
from abc import ABC,abstractmethod
import datetime
from backoff import on_exception,expo
import ratelimit
from ingestion.Exceptions import DateDataIngestionNotFoundException

# %%
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MercadoBitcoinAPI(ABC):

    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.base_endpoint = 'https://www.mercadobitcoin.net/api'

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass
        # return f"{self.base_endpoint}/BTC/day-summary/2022/06/05"

    @on_exception(expo,ratelimit.RateLimitException,max_tries=10)
    @ratelimit.limits(calls=29,period=30)
    @on_exception(expo, req.exceptions.HTTPError,max_tries=10)
    def get_data(self, **kwargs) -> Union[dict, list[dict]]:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = req.get(endpoint)
        response.raise_for_status()
        return response.json()

# %%


class DaySummary(MercadoBitcoinAPI):
    type = 'day-summary'

    def _get_endpoint(self, date: datetime.date) -> str:
        self.date = date
        return f"{self.base_endpoint}/{self.coin}/{self.type}/{self.date.year}/{self.date.month}/{self.date.day}"
# %%


class Trades(MercadoBitcoinAPI):
    type = 'trades'

    def _get_unix_date(self, date: datetime.datetime) -> int:
        return int(date.timestamp())

    def _get_endpoint(self, date_start: datetime.datetime = None, date_to: datetime.datetime = None) -> str:

        if date_start and date_to:
            self.date_start = self._get_unix_date(date_start)
            self.date_to = self._get_unix_date(date_to)
            return f"{self.base_endpoint}/{self.coin}/{self.type}/{self.date_start}/{self.date_to}"
        elif date_start and not date_to:
            self.date_start = self._get_unix_date(date_start)
            return f"{self.base_endpoint}/{self.coin}/{self.type}/{self.date_start}"
        else:
            raise DateDataIngestionNotFoundException(f"A date start must be specified")