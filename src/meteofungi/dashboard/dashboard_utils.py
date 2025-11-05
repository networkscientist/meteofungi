import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import plotly.express as px
import polars as pl
import streamlit as st

from meteofungi.constants import (
    DATA_PATH,
    TIMEZONE_SWITZERLAND_STRING,
    parameter_description_extraction_pattern,
)
from meteofungi.dashboard.constants import (
    METRICS_STRINGS,
    NUM_DAYS_DELTA,
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


META_PARAMETERS: pl.LazyFrame = load_metadata_to_frame('parameters')
META_STATIONS: pl.LazyFrame = load_metadata_to_frame('stations')


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


METRICS_NAMES_DICT: dict[str, str] = {
    m: create_meta_map(META_PARAMETERS).get(m, '') for m in METRICS_STRINGS
}
WEATHER_COLUMN_NAMES_DICT: dict[str, str] = dict(
    {'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT
)


def create_stations_options_selected(station_name_list):
    return st.multiselect(
        label='Stations:',
        options=station_name_list,
        default='Airolo',
        max_selections=SIDEBAR_MAX_SELECTIONS,
        placeholder='Choose Station(s)',
    )


def create_area_chart_frame(frame_weather, stations_options_selected):
    return (
        frame_weather.sort('reference_timestamp')
        .filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                    - timedelta(days=NUM_DAYS_DELTA)
                )
            )
            & (pl.col('station_name').is_in(stations_options_selected))
        )
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_name')
        .agg(
            pl.sum(*PARAMETER_AGGREGATION_TYPES['sum']),
            pl.mean(*PARAMETER_AGGREGATION_TYPES['mean']),
        )
        .with_columns(pl.selectors.numeric().round(1))
        .rename(WEATHER_COLUMN_NAMES_DICT)
    )


scatter_map_kwargs: dict[str, str | dict[str, bool] | list[str | Any] | int] = {
    'lat': 'station_coordinates_wgs84_lat',
    'lon': 'station_coordinates_wgs84_lon',
    'color': 'Station Type',
    'hover_name': 'station_name',
    'hover_data': {
        'Station Type': False,
        'station_coordinates_wgs84_lat': False,
        'station_coordinates_wgs84_lon': False,
        'Short Code': True,
        'Altitude': True,
    },
    'color_continuous_scale': px.colors.cyclical.IceFire,
    'size_max': 15,
    'zoom': 6,
    'map_style': 'light',
    'title': 'Station Locations',
}
