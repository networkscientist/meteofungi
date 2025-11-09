import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import polars as pl
import streamlit as st

from meteofungi.constants import (
    DATA_PATH,
    TIMEZONE_SWITZERLAND_STRING,
    parameter_description_extraction_pattern,
)
from meteofungi.dashboard.constants import (
    METRICS_STRINGS,
    PARAMETER_AGGREGATION_TYPES,
    SIDEBAR_MAX_SELECTIONS,
)


@st.cache_data
def load_metadata_to_frame(meta_type: str) -> pl.LazyFrame:
    """Load  metadata

    Returns
    -------
        Metadata in Polars LazyFrame
    """
    return pl.scan_parquet(
        Path(DATA_PATH, f'meta_{meta_type.lower()}.parquet')
    ).unique()


def collect_meta_params_to_dicts(metadata):
    rows: tuple[dict[str, Any]] = tuple(
        metadata.collect().to_dicts(),
    )
    return rows


def create_meta_map(metadata):
    meta_map: dict = {
        r['parameter_shortname']: re.search(
            parameter_description_extraction_pattern, r['parameter_description_en']
        ).group()
        for r in collect_meta_params_to_dicts(metadata)
    }
    return meta_map


def create_stations_options_selected(station_name_list):
    return st.multiselect(
        label='Stations:',
        options=station_name_list,
        default='Airolo',
        max_selections=SIDEBAR_MAX_SELECTIONS,
        placeholder='Choose Station(s)',
    )


@st.cache_data
def create_metrics(
    _weather_data: pl.LazyFrame, time_periods: Mapping[int, datetime]
) -> pl.LazyFrame:
    return pl.concat(
        [
            _weather_data.filter(pl.col('reference_timestamp') >= datetime_period)
            .drop('reference_timestamp')
            .group_by(('station_abbr', 'station_name'))
            .agg(
                pl.sum(*PARAMETER_AGGREGATION_TYPES['sum']),
                pl.mean(*PARAMETER_AGGREGATION_TYPES['mean']),
            )
            .with_columns(pl.lit(period).alias('time_period'))
            for period, datetime_period in time_periods.items()
        ]
    )


@st.cache_data
def create_station_name_list(_frame_with_stations: pl.LazyFrame) -> tuple[str, ...]:
    return tuple(
        _frame_with_stations.unique(subset=('station_name',))
        .sort('station_name')
        .select('station_name')
        .collect()
        .to_series()
        .to_list()
    )


@st.cache_data
def create_station_frame_for_map(_frame_with_stations: pl.LazyFrame) -> pl.DataFrame:
    return (
        _frame_with_stations.with_columns(
            pl.col('station_type_en').alias('Station Type'),
            pl.col('station_abbr').alias('Short Code'),
            Altitude=pl.col('station_height_masl')
            .cast(pl.Int16)
            .cast(pl.String)
            .add(' m.a.s.l'),
        )
        .select(
            pl.col(
                (
                    'Short Code',
                    'Station Type',
                    'station_name',
                    'station_coordinates_wgs84_lat',
                    'station_coordinates_wgs84_lon',
                    'Altitude',
                )
            )
        )
        .collect()
    )


@st.cache_data
def load_weather_data() -> pl.LazyFrame:
    return pl.scan_parquet(Path(DATA_PATH, 'weather_data.parquet')).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            TIMEZONE_SWITZERLAND_STRING, non_existent='null'
        )
    )


META_PARAMETERS: pl.LazyFrame = load_metadata_to_frame('parameters')
META_STATIONS: pl.LazyFrame = load_metadata_to_frame('stations')
METRICS_NAMES_DICT: dict[str, str] = {
    m: create_meta_map(META_PARAMETERS).get(m, '') for m in METRICS_STRINGS
}
WEATHER_COLUMN_NAMES_DICT: dict[str, str] = dict(
    {'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT
)
