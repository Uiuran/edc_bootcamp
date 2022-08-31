import datetime
import json
from typing import Union
from ingestion.Exceptions import WarningWrongDataTypeNotWritable
from ingestion.api import MercadoBitcoinAPI
import os

class DataWriter:

    def __init__(self, coin: str, api: MercadoBitcoinAPI) -> None:
        self.partition = f"{coin}/{api.type}/"
        self.filename = self.partition+f"{datetime.datetime.now()}"

    def write(self, data: Union[list[dict], dict]) -> None:
        os.makedirs(self.partition, exist_ok=True)
        if isinstance(data, dict):
            with open(self.filename, 'a') as f:
                f.write(json.dumps(data)+"\n")
        elif isinstance(data, list):
            for datum in data:
                if isinstance(datum, dict):
                    with open(self.filename, 'a') as f:
                        f.write(json.dumps(datum)+"\n")
                else:
                    warn = WarningWrongDataTypeNotWritable()
                    warn.warns(
                        f"Data type {datum.__class__} not allowed for writing, only dict is allowed. This datum will not be writed")

#