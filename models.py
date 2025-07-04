from typing import List, Optional, Any
from pydantic import BaseModel, field_validator
import datetime


class ValueModel(BaseModel):
    intVal: Optional[int] = None
    fpVal: Optional[float] = None
    mapVal: Optional[Any] = None


class PointModel(BaseModel):
    startTimeNanos: Optional[int] = None
    endTimeNanos: Optional[int] = None
    dataTypeName: Optional[str] = None
    originDataSourceId: Optional[str] = None
    value: List[ValueModel] = []

    @field_validator("startTimeNanos", "endTimeNanos", mode="before")
    @staticmethod
    def parse_int_fields(v):
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid timestamp value: {v}")

    def to_interval(self) -> dict:
        """Convert nanosecond timestamps to ISO datetime strings"""

        def nanos_to_utc(nanos: int) -> str:
            ts_sec = nanos / 1_000_000_000
            dt = datetime.datetime.utcfromtimestamp(ts_sec)
            micros = int(nanos % 1_000_000_000) // 1000
            return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{micros:06d}Z"

        return {
            "start": (
                nanos_to_utc(self.startTimeNanos)
                if self.startTimeNanos is not None
                else None
            ),
            "end": (
                nanos_to_utc(self.endTimeNanos)
                if self.endTimeNanos is not None
                else None
            ),
        }


class DataSetModel(BaseModel):
    dataSourceId: Optional[str] = None
    point: List[PointModel] = []


class BucketModel(BaseModel):
    startTimeMillis: Optional[int] = None
    endTimeMillis: Optional[int] = None
    dataset: List[DataSetModel] = []

    @field_validator("startTimeMillis", "endTimeMillis", mode="before")
    @staticmethod
    def parse_millis(v):
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid millisecond timestamp: {v}")

    def get_sleep_records(self) -> List[dict]:
        """
        Flatten all points into a list of dicts with interval and value.
        """
        records = []
        for ds in self.dataset:
            for pt in ds.point:
                if not pt.value:
                    continue
                first = pt.value[0]
                val = first.intVal if first.intVal is not None else first.fpVal
                records.append({**pt.to_interval(), "value": val})
        return records


class RecordModel(BaseModel):
    startTimeMillis: Optional[int]
    endTimeMillis: Optional[int]
    dataset: List[DataSetModel] = []
