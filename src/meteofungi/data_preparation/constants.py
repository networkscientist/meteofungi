from pathlib import Path

import polars as pl

DATA_PATH: Path = Path(__file__).resolve().parents[3].joinpath('data')
DTYPE_DICT: dict[str, type[pl.DataType]] = {
    'Integer': pl.Int16,
    'Float': pl.Float32,
    'String': pl.String,
}

METEO_CSV_ENCODING = 'ISO-8859-1'
META_FILE_PATH_DICT: dict[str, list[str]] = {
    meta_type: [
        f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn{ogd_smn_prefix}/ogd-smn{meta_suffix}_meta_{meta_type}.csv'
        for ogd_smn_prefix, meta_suffix in zip(
            ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
        )
    ]
    for meta_type in ['stations', 'parameters', 'datainventory']
}

SCHEMA_META_STATIONS: dict[str, type[pl.DataType]] = {
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

SCHEMA_META_PARAMETERS: dict[str, type[pl.DataType]] = {
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

SCHEMA_META_DATAINVENTORY: dict[str, type[pl.DataType]] = {
    'station_abbr': pl.String,
    'parameter_shortname': pl.String,
    'meas_cat_nr': pl.Int8,
    'data_since': pl.Datetime,
    'data_till': pl.Datetime,
    'owner': pl.String,
}

COLS_TO_KEEP_META_STATIONS: tuple[str, ...] = (
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

COLS_TO_KEEP_META_PARAMETERS: tuple[str, ...] = (
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

COLS_TO_KEEP_META_DATAINVENTORY: tuple[str, ...] = (
    'data_since',
    'data_till',
    'owner',
    'parameter_shortname',
    'station_abbr',
)
URL_GEO_ADMIN_BASE = 'https://data.geo.admin.ch'
URL_GEO_ADMIN_STATION_TYPE_BASE = 'ch.meteoschweiz.ogd-smn'
