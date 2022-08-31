# %%
from abc import ABC, abstractmethod
import datetime
import time
from timeit import repeat
from typing import Union
from ingestion.Exceptions import StartDateNotProvidedException, WarningDateProvidedLowerThanCheckpoint
from ingestion.api import DaySummary
from ingestion.writer import DataWriter

class IngestorAPI(ABC):

    def __init__(self, coins: list[str]) -> None:
        super().__init__()
        self.coins = coins
        self._checkpoint = self._load_checkpoint()
        self.date = None

    @abstractmethod
    def ingestion(self, **kwargs) -> None:
        pass

    @property
    def _checkpoint_filename(self):
        return f"{self.__class__.__name__}.checkpoint"

    def _load_checkpoint(self) -> datetime.date:
        try:
            with open(self._checkpoint_filename, 'r') as f:
                return datetime.datetime.strptime(f.read(), "%Y-%m-%d").date()
        except:
            return None

    def _write_checkpoint(self):

        with open(self._checkpoint_filename, 'w') as f:
            f.write(f"{self._checkpoint}")

    def _get_checkpoint(self) -> Union[datetime.date, datetime.datetime]:

        if not self._checkpoint:
            return self.date
        else:

            return self._checkpoint

    def _update_checkpoint(self, value: Union[datetime.date, datetime.datetime]) -> None:
        self._checkpoint = value

# %%


class DaySummaryIngestor(IngestorAPI):

    def ingestion(self, date: datetime.date = None) -> None:
        if not date and not self._checkpoint:
            raise StartDateNotProvidedException(
                "Start date is None and there is no checkpoint, an initial value must be provided")
        elif not date and self._checkpoint:
            self.date = self._get_checkpoint()
        else:

            if not self._checkpoint:
                self.date = date
            elif date >= self._checkpoint:
                self.date = date
            elif date < self._checkpoint:
                warn = WarningDateProvidedLowerThanCheckpoint()
                warn.warns(
                    f"{date} is lower than {self._checkpoint}, using checkpoint instead. If you want to restart the ingestion you must delete the checkpoint")
                self.date = self._get_checkpoint()

        if self.date < datetime.date.today():
            for coin in self.coins:
                day_summary = DaySummary(coin=coin)
                writer = DataWriter(coin=coin, api=day_summary)
                writer.write(day_summary.get_data(date=self.date))
            self._update_checkpoint(self.date + datetime.timedelta(days=1))
            self._write_checkpoint()