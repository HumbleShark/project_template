from csv import DictReader
from datetime import datetime
from contextlib import contextmanager

from domain.aggregated_data import AggregatedData
from marshmallow import Schema

from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema

import config


class FileDatasource:
    def __init__(self) -> None:
        self.readers = {
            'GPS': Reader('data/gps.csv', GpsSchema()),
            'ACCELEROMETER': Reader(
                'data/accelerometer.csv', AccelerometerSchema()
            ),
        }

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        try:
            accelerometer_data = self.readers['ACCELEROMETER'].read()
            gps_data = self.readers['GPS'].read()
            time = datetime.now()
            user_id = config.USER_ID
            return AggregatedData(
                user_id=user_id, accelerometer=accelerometer_data, gps=gps_data, timestamp=time
            )
        except Exception as e:
            print(f"Exception occurred: {e}")

    @contextmanager
    def start_reading(self):
        """Метод повинен викликатись перед початком читання даних"""
        try:
            for reader in self.readers.values():
                reader.start_reading()
            yield
        except Exception as e:
            print(f"Exception occurred: {e}")
        finally:
            self.stop_reading()
                
    def stop_reading(self):
        """Метод повинен викликатись для закінчення читання даних"""
        for reader in self.readers.values():
            reader.stop_reading()


class Reader:
    def __init__(self, filename, schema: Schema):
        self.filename = filename
        self.schema = schema
        self.file = None
        self.reader = None

    def start_reading(self):
        self.file = open(self.filename, "r")
        self.reader = DictReader(self.file)
        return self.reader

    def read(self):
        row = next(self.reader, None)

        if row is None:
            self.file.seek(0)
            row = next(self.reader, None)

        return self.schema.load(row)

    def stop_reading(self):
        if self.file:
            self.file.close()
