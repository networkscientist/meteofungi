"""Prepare data for the MeteoShrooms dashboard"""

import argparse
import logging
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Mapping
from zoneinfo import ZoneInfo

import polars as pl

from meteofungi.constants import DATA_PATH, TIMEZONE_SWITZERLAND_STRING
from meteofungi.data_preparation.constants import (
    COLS_TO_KEEP_META_DATAINVENTORY,
    COLS_TO_KEEP_META_PARAMETERS,
    COLS_TO_KEEP_META_STATIONS,
    DTYPE_DICT,
    EXPR_WEATHER_AGGREGATION_TYPES,
    META_FILE_PATH_DICT,
    METEO_CSV_ENCODING,
    PARAMETER_AGGREGATION_TYPES,
    SCHEMA_META_DATAINVENTORY,
    SCHEMA_META_PARAMETERS,
    SCHEMA_META_STATIONS,
    STATION_TYPE_ERROR_STRING,
    TIME_PERIODS,
    TIMEFRAME_STRINGS,
    TIMEFRAME_VALUE_ERROR_STRING,
    URL_GEO_ADMIN_BASE,
    URL_GEO_ADMIN_STATION_TYPE_BASE,
)

logger: logging.Logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)


def load_metadata(
    meta_type: str,
    file_path_dict: dict[str, list[str]],
    meta_schema: Mapping[str, type[pl.DataType]],
    meta_cols_to_keep: Sequence[str],
    data_path: Path = DATA_PATH,
) -> pl.LazyFrame:
    """Load metadata from a Parquet file.

    Parameters
    ----------
    data_path
    meta_type: str
        Metadata source, one of 'parameters', 'frame_meta' or 'datainventory'
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
    frame_meta: pl.LazyFrame = pl.concat(
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
    ).lazy()
    logger.debug(f'frame_meta with type {meta_type} as pl.LazyFrame created')
    logger.debug('Try to write frame_meta to parquet')
    frame_meta.sink_parquet(Path(data_path, f'meta_{meta_type}.parquet'))
    logger.debug('frame_meta written to parquet')
    return frame_meta


def combine_urls_parts_to_string(
    station: pl.Series, station_type_string: str, timeframe: Literal['recent', 'now']
) -> pl.Series:
    return (
        f'{URL_GEO_ADMIN_BASE}/{URL_GEO_ADMIN_STATION_TYPE_BASE}{station_type_string}/'
        + station
        + f'/ogd-smn{station_type_string}_'
        + station
        + f'_h_{timeframe}.csv'
    )


def generate_download_urls(
    station_series: pl.Series, station_type: str, timeframe: str
) -> pl.Series:
    if timeframe not in TIMEFRAME_STRINGS:
        raise ValueError(TIMEFRAME_VALUE_ERROR_STRING)
    if not isinstance(station_type, str):
        raise TypeError(STATION_TYPE_ERROR_STRING)
    match station_type:
        case 'rainfall':
            station_type_string = '-precip'
        case 'weather':
            station_type_string = ''
    return combine_urls_parts_to_string(station_series, station_type_string, timeframe)


def expr_filter_column_timedelta(col_name, delta_time):
    return pl.col(col_name) >= pl.lit(
        datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
        - timedelta(days=delta_time)
    )


def load_weather(
    metadata: pl.LazyFrame,
    schema_dict_lazyframe: Mapping[str, type[pl.DataType]],
    from_disk=False,
) -> pl.LazyFrame:
    stations = filter_unique_station_names(metadata).collect()
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
    if from_disk:
        weather = pl.scan_parquet(Path(DATA_PATH, 'weather_data.parquet'))
        urls_weather = generate_download_urls(station_series_weather, 'weather', 'now')
        urls_rainfall = generate_download_urls(
            station_series_precipitation, 'rainfall', 'now'
        )
        weather_now = create_rainfall_weather_lazyframes(urls_weather, kwargs_lazyframe)
        rainfall_now = create_rainfall_weather_lazyframes(
            urls_rainfall, kwargs_lazyframe
        )
        weather_new = concat_rainfall_weather_lazyframes(
            metadata, rainfall_now, weather_now
        )
        return (
            pl.concat(
                (
                    weather_new.filter(
                        pl.col('reference_timestamp')
                        > weather.select('reference_timestamp').max().collect().item()
                    )
                    .select(weather.drop('station_name').collect_schema().names())
                    .join(
                        metadata.select(('station_abbr', 'station_name')),
                        on=['station_abbr'],
                    ),
                    weather,
                )
            )
            .filter(expr_filter_column_timedelta('reference_timestamp', 31))
            .unique()
        )
    else:
        urls_weather = pl.concat(
            generate_download_urls(station_series_weather, 'weather', period)
            for period in TIMEFRAME_STRINGS
        )
        urls_rainfall = pl.concat(
            generate_download_urls(station_series_precipitation, 'rainfall', period)
            for period in TIMEFRAME_STRINGS
        )
        weather: pl.LazyFrame = create_rainfall_weather_lazyframes(
            urls_weather, kwargs_lazyframe
        )
        rainfall: pl.LazyFrame = create_rainfall_weather_lazyframes(
            urls_rainfall, kwargs_lazyframe
        )
        return concat_rainfall_weather_lazyframes(metadata, rainfall, weather)


def concat_rainfall_weather_lazyframes(
    metadata: pl.LazyFrame, frame_rainfall: pl.LazyFrame, frame_weather: pl.LazyFrame
) -> pl.LazyFrame:
    return (
        pl.concat([frame_rainfall, frame_weather], how='diagonal')
        .sort('reference_timestamp')
        .filter(expr_filter_column_timedelta('reference_timestamp', 31))
        .group_by_dynamic('reference_timestamp', every='1h', group_by='station_abbr')
        .agg(*EXPR_WEATHER_AGGREGATION_TYPES)
        .join(
            metadata.select(('station_abbr', 'station_name')),
            on=['station_abbr'],
        )
        .unique()
    )


def filter_unique_station_names(metadata: pl.LazyFrame) -> pl.LazyFrame:
    return (
        metadata.select('station_abbr', 'station_type_en')
        .unique('station_abbr')
        .sort('station_abbr')
    )


def create_rainfall_weather_lazyframes(urls, kwargs_lazyframe: dict) -> pl.LazyFrame:
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
        urls.to_list(),
        **kwargs_lazyframe,
    ).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            TIMEZONE_SWITZERLAND_STRING, non_existent='null', ambiguous='earliest'
        )
    )


EXPR_METRICS_AGGREGATION_TYPE_WHEN_THEN = (
    pl.when(pl.col('parameter').is_in(PARAMETER_AGGREGATION_TYPES['sum']))
    .then(pl.lit('sum'))
    .otherwise(pl.lit('mean'))
    .alias('type')
)


def create_metrics(
    weather_data: pl.LazyFrame, time_periods: Mapping[int, datetime]
) -> pl.LazyFrame:
    return (
        pl.concat(
            [
                weather_data.filter(pl.col('reference_timestamp') >= datetime_period)
                .drop(
                    'reference_timestamp',
                )
                .group_by(('station_abbr', 'station_name'))
                .agg(*EXPR_WEATHER_AGGREGATION_TYPES)
                .with_columns(pl.lit(period).alias('time_period').cast(pl.Int8))
                for period, datetime_period in time_periods.items()
            ]
        )
        .unpivot(
            index=('station_abbr', 'station_name', 'time_period'),
            variable_name='parameter',
        )
        .drop_nulls('value')
        .with_columns(EXPR_METRICS_AGGREGATION_TYPE_WHEN_THEN)
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


def create_weather_schema_dict() -> dict[Any, type[pl.DataType]]:
    return {
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--metrics', action='store_true')
    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-f', '--fulldownload', action='store_true')
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.debug('Logger created')
    weather_schema_dict: dict[str, type[pl.DataType]] = create_weather_schema_dict()
    meta_stations: pl.LazyFrame = (
        load_metadata(
            'stations',
            META_FILE_PATH_DICT,
            SCHEMA_META_STATIONS,
            COLS_TO_KEEP_META_STATIONS,
        )
        .collect()
        .lazy()
    )
    meta_datainventory: pl.LazyFrame = (
        load_metadata(
            'datainventory',
            META_FILE_PATH_DICT,
            SCHEMA_META_DATAINVENTORY,
            COLS_TO_KEEP_META_DATAINVENTORY,
        )
        .collect()
        .lazy()
    )
    weather_data: pl.LazyFrame = (
        load_weather(
            meta_stations,
            schema_dict_lazyframe=weather_schema_dict,
            from_disk=args.fulldownload,
        )
        .collect()
        .lazy()
    )
    if args.metrics:
        metrics = create_metrics(weather_data, TIME_PERIODS)
        metrics.sink_parquet(
            Path(DATA_PATH, 'metrics.parquet'),
            compression='brotli',
            compression_level=11,
        )
    weather_data.sink_parquet(
        Path(DATA_PATH, 'weather_data.parquet'),
        compression='brotli',
        compression_level=11,
    )
