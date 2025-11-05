"""Prepare data for the MeteoShrooms dashboard"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

import polars as pl

from meteofungi.constants import DATA_PATH, TIMEZONE_SWITZERLAND_STRING
from meteofungi.data_preparation.constants import (
    COLS_TO_KEEP_META_DATAINVENTORY,
    COLS_TO_KEEP_META_PARAMETERS,
    COLS_TO_KEEP_META_STATIONS,
    DTYPE_DICT,
    META_FILE_PATH_DICT,
    METEO_CSV_ENCODING,
    SCHEMA_META_DATAINVENTORY,
    SCHEMA_META_PARAMETERS,
    SCHEMA_META_STATIONS,
)


def load_metadata(
    meta_type: str,
    file_path_dict: dict[str, list[str]],
    meta_schema: Mapping[str, type[pl.DataType]],
    meta_cols_to_keep: Sequence[str],
) -> pl.LazyFrame:
    """Load metadata from a Parquet file.

    Parameters
    ----------
    meta_type: str
        Metadata source, one of 'parameters', 'stations' or 'datainventory'
    file_path_dict: dict[str, list[str]]
        Dict with URLs to metadata
    meta_schema: dict
        Dict with polars schema, structured as 'column_name': polars.Datatype
    meta_cols_to_keep: Sequence[str]
        Column names to keep in metadata DataFrame

    Returns
    -------
        Metadata loaded into a LazyFrame
    """
    stations: pl.DataFrame = pl.concat(
        [
            pl.read_csv(
                file_path,
                encoding=METEO_CSV_ENCODING,
                separator=';',
                schema=meta_schema,
                columns=meta_cols_to_keep,
            )
            for file_path in file_path_dict[meta_type]
        ]
    )
    stations.write_parquet(Path(DATA_PATH, f'meta_{meta_type}.parquet'))
    return stations.lazy()


def generate_download_url(station: str, station_type: str, timeframe: str) -> str:
    if timeframe not in ['recent', 'now']:
        timeframe_value_error_string = "timeframe needs to be 'recent' or 'now'"
        raise ValueError(timeframe_value_error_string)
    match station_type:
        case 'rainfall':
            return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_h_{timeframe}.csv'
        case 'weather':
            return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station}/ogd-smn_{station}_h_{timeframe}.csv'
    station_type_type_error_string = 'station_type must be String and cannot be None'
    raise TypeError(station_type_type_error_string)


def load_weather(
    metadata: pl.LazyFrame, schema_dict_lazyframe: Mapping[str, type[pl.DataType]]
) -> pl.LazyFrame:
    stations: pl.DataFrame = (
        metadata.select('station_abbr', 'station_type_en')
        .unique('station_abbr')
        .sort('station_abbr')
        .collect()
    )
    kwargs_lazyframe: dict = {
        'separator': ';',
        'try_parse_dates': True,
        'schema_overrides': schema_dict_lazyframe,
    }
    station_series_precipitation: pl.Series = filter_stations_to_series(
        stations, station_type='Automatic precipitation stations'
    )
    station_series_weather: pl.Series = filter_stations_to_series(
        stations, station_type='Automatic weather stations'
    )
    rainfall_recent: pl.LazyFrame = create_rainfall_weather_lazyframes(
        station_series_precipitation, 'rainfall', 'recent', kwargs_lazyframe
    )
    rainfall_now: pl.LazyFrame = create_rainfall_weather_lazyframes(
        station_series_precipitation, 'rainfall', 'now', kwargs_lazyframe
    )
    weather_recent: pl.LazyFrame = create_rainfall_weather_lazyframes(
        station_series_weather, 'weather', 'recent', kwargs_lazyframe
    )
    weather_now: pl.LazyFrame = create_rainfall_weather_lazyframes(
        station_series_weather, 'weather', 'now', kwargs_lazyframe
    )
    rainfall: pl.LazyFrame = pl.concat(
        [
            rainfall_recent,
            rainfall_now,
        ]
    )
    weather: pl.LazyFrame = pl.concat(
        [
            weather_recent,
            weather_now,
        ],
    )
    return (
        pl.concat([rainfall, weather], how='diagonal')
        .sort('reference_timestamp')
        .filter(
            pl.col('reference_timestamp')
            >= pl.lit(
                datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                - timedelta(days=31)
            )
        )
        .group_by_dynamic('reference_timestamp', every='1h', group_by='station_abbr')
        .agg(
            pl.sum('rre150h0'), pl.mean('tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0')
        )
        .join(metadata.select(('station_abbr', 'station_name')), on=['station_abbr'])
    )


def create_rainfall_weather_lazyframes(
    station_series: pl.Series, station_type: str, timeframe: str, kwargs_lazyframe: dict
) -> pl.LazyFrame:
    """Create LazyFrame from CSV urls

    Parameters
    ----------
    station_series: pl.Series
        Station names
    station_type: str
        Station type, one of 'rainfall' or 'weather'
    timeframe: str
        Time range, one of 'recent' or 'now'
    kwargs_lazyframe: dict
        Arguments to pass to LazyFrame constructor

    Returns
    -------
        Polars LazyFrame with rainfall/weather data
    """
    return pl.scan_csv(
        [
            generate_download_url(station, station_type, timeframe)
            for station in station_series
        ],
        **kwargs_lazyframe,
    ).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            TIMEZONE_SWITZERLAND_STRING, non_existent='null', ambiguous='earliest'
        )
    )


def filter_stations_to_series(stations: pl.DataFrame, station_type: str) -> pl.Series:
    """Filter stations according to station type

    Parameters
    ----------
    stations: pl.DataFrame
        Weather stations DataFrame
    station_type
        Station type, one of 'rainfall' or 'weather'

    Returns
    -------
        Polars Series with station names
    """
    return (
        stations.filter(pl.col('station_type_en') == station_type)
        .select('station_abbr')
        .to_series()
        .str.to_lowercase()
    )


if __name__ == '__main__':
    weather_schema_dict: dict[str, type[pl.DataType]] = {
        colname: DTYPE_DICT[datatype]
        for colname, datatype in load_metadata(
            'parameters',
            META_FILE_PATH_DICT,
            SCHEMA_META_PARAMETERS,
            COLS_TO_KEEP_META_PARAMETERS,
        )
        .select(pl.col('parameter_shortname'), pl.col('parameter_datatype'))
        .collect()
        .iter_rows()
    }

    meta_stations: pl.LazyFrame = load_metadata(
        'stations',
        META_FILE_PATH_DICT,
        SCHEMA_META_STATIONS,
        COLS_TO_KEEP_META_STATIONS,
    )
    meta_datainventory: pl.LazyFrame = load_metadata(
        'datainventory',
        META_FILE_PATH_DICT,
        SCHEMA_META_DATAINVENTORY,
        COLS_TO_KEEP_META_DATAINVENTORY,
    )
    weather_data: pl.LazyFrame = load_weather(
        meta_stations, schema_dict_lazyframe=weather_schema_dict
    )
    weather_data.sink_parquet(
        Path(DATA_PATH, 'weather_data.parquet'),
        compression='brotli',
        compression_level=11,
    )
