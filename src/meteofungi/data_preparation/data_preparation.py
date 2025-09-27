"""Prepare data for the MeteoShrooms dashboard"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

DATA_PATH: Path = Path(__file__).resolve().parents[3].joinpath('data')
DTYPE_DICT: dict = {'Integer': pl.Int16, 'Float': pl.Float32, 'String': pl.String}

METEO_CSV_ENCODING: str = 'ISO-8859-1'
META_FILE_PATH_DICT: dict[str, list[str]] = {
    meta_type: [
        f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn{ogd_smn_prefix}/ogd-smn{meta_suffix}_meta_{meta_type}.csv'
        for ogd_smn_prefix, meta_suffix in zip(
            ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
        )
    ]
    for meta_type in ['stations', 'parameters', 'datainventory']
}

SCHEMA_META_STATIONS: dict = {
    'station_abbr': pl.String,
    'station_name': pl.String,
    'station_canton': pl.String,
    'station_wigos_id': pl.String,
    'station_type_de': pl.String,
    'station_type_fr': pl.String,
    'station_type_it': pl.String,
    'station_type_en': pl.String,
    'station_dataowner': pl.String,
    'station_data_since': pl.String,
    'station_height_masl': pl.Float64,
    'station_height_barometer_masl': pl.Float64,
    'station_coordinates_lv95_east': pl.Float64,
    'station_coordinates_lv95_north': pl.Float64,
    'station_coordinates_wgs84_lat': pl.Float64,
    'station_coordinates_wgs84_lon': pl.Float64,
    'station_exposition_de': pl.String,
    'station_exposition_fr': pl.String,
    'station_exposition_it': pl.String,
    'station_exposition_en': pl.String,
    'station_url_de': pl.String,
    'station_url_fr': pl.String,
    'station_url_it': pl.String,
    'station_url_en': pl.String,
}

SCHEMA_META_PARAMETERS: dict = {
    'parameter_shortname': pl.String,
    'parameter_description_de': pl.String,
    'parameter_description_fr': pl.String,
    'parameter_description_it': pl.String,
    'parameter_description_en': pl.String,
    'parameter_group_de': pl.String,
    'parameter_group_fr': pl.String,
    'parameter_group_it': pl.String,
    'parameter_group_en': pl.String,
    'parameter_granularity': pl.String,
    'parameter_decimals': pl.Int8,
    'parameter_datatype': pl.String,
    'parameter_unit': pl.String,
}

SCHEMA_META_DATAINVENTORY: dict = {
    'station_abbr': pl.String,
    'parameter_shortname': pl.String,
    'meas_cat_nr': pl.Int8,
    'data_since': pl.Datetime,
    'data_till': pl.Datetime,
    'owner': pl.String,
}

COLS_TO_KEEP_META_STATIONS: Sequence[str] = (
    'station_abbr',
    'station_name',
    'station_canton',
    'station_type_de',
    'station_type_en',
    'station_dataowner',
    'station_data_since',
    'station_height_masl',
    'station_height_barometer_masl',
    'station_coordinates_lv95_east',
    'station_coordinates_lv95_north',
    'station_coordinates_wgs84_lat',
    'station_coordinates_wgs84_lon',
)

COLS_TO_KEEP_META_PARAMETERS: Sequence[str] = (
    'parameter_datatype',
    'parameter_decimals',
    'parameter_description_de',
    'parameter_description_en',
    'parameter_granularity',
    'parameter_group_de',
    'parameter_group_en',
    'parameter_shortname',
    'parameter_unit',
)

COLS_TO_KEEP_META_DATAINVENTORY: Sequence[str] = (
    'data_since',
    'data_till',
    'owner',
    'parameter_shortname',
    'station_abbr',
)


def load_metadata(
    meta_type: str,
    file_path_dict: dict[str, list[str]],
    meta_schema: dict,
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


def load_weather(metadata: pl.LazyFrame, schema_dict_lazyframe: dict) -> pl.LazyFrame:
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
            >= pl.lit(datetime.now(tz=ZoneInfo('Europe/Zurich')) - timedelta(days=31))
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
            'Europe/Zurich', non_existent='null'
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
    weather_schema_dict: dict = {
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
