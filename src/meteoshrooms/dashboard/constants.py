from datetime import datetime, timedelta
from itertools import chain
from zoneinfo import ZoneInfo

from meteoshrooms.constants import TIME_PERIOD_VALUES, TIMEZONE_SWITZERLAND_STRING

TIME_PERIODS: dict[int, datetime] = {
    period: (
        datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING)) - timedelta(days=period)
    )
    for period in TIME_PERIOD_VALUES
}
NUM_DAYS_VAL: int = next(iter(TIME_PERIODS.keys()))
NUM_DAYS_DELTA: int = tuple(TIME_PERIODS.keys())[1]
PARAMETER_AGGREGATION_TYPES: dict[str, tuple[str, ...]] = {
    'sum': ('rre150h0',),
    'mean': ('tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0'),
}
METRICS_STRINGS: tuple[str, ...] = tuple(
    chain.from_iterable(PARAMETER_AGGREGATION_TYPES.values())
)

WEATHER_SHORT_LABEL_DICT: dict[str, str] = {
    'rre150h0': 'Precipitation',
    'tre200h0': 'Air Temperature',
    'ure200h0': 'Rel. Humidity',
    'fu3010h0': 'Wind Speed',
    'tde200h0': 'Dew Point',
}
SIDEBAR_MAX_SELECTIONS: int = 5
COLUMNS_FOR_MAP_FRAME: set = {
    'Short Code',
    'Station Type',
    'station_name',
    'station_coordinates_wgs84_lat',
    'station_coordinates_wgs84_lon',
    'Altitude',
}
