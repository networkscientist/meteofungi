import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from re import Pattern
from typing import Any
from zoneinfo import ZoneInfo

import plotly.express as px
import polars as pl
import streamlit as st
from plotly.graph_objs import Figure
from polars import DataFrame
from streamlit.delta_generator import DeltaGenerator

from meteofungi.dashboard.ux_metrics import (
    create_metrics_expander_info,
    get_metric_emoji,
)

DATA_PATH: Path = Path(__file__).resolve().parents[3].joinpath('data')
# --- Load data ---
st.set_page_config(layout='wide', initial_sidebar_state='expanded')

TIME_PERIODS: dict[int, datetime] = {
    period: (datetime.now(tz=ZoneInfo('Europe/Zurich')) - timedelta(days=period))
    for period in (3, 7, 14, 30)
}
NUM_DAYS_VAL: int = next(iter(TIME_PERIODS.keys()))
NUM_DAYS_DELTA: int = tuple(TIME_PERIODS.keys())[1]
PARAMETER_AGGREGATION_TYPES: dict[str, list[str]] = {
    'sum': ['rre150h0'],
    'mean': ['tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0'],
}
METRICS_LIST: Sequence[str] = tuple(
    chain.from_iterable(PARAMETER_AGGREGATION_TYPES.values())
)


@st.cache_data
def load_weather_data() -> pl.LazyFrame:
    return pl.scan_parquet(Path(DATA_PATH, 'weather_data.parquet')).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            'Europe/Zurich', non_existent='null'
        )
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
def load_meta_stations() -> pl.LazyFrame:
    """Load station metadata

    Returns
    -------
        Station metadata in Polars LazyFrame
    """
    return pl.scan_parquet(Path(DATA_PATH, 'meta_stations.parquet'))


@st.cache_data
def load_meta_params() -> pl.LazyFrame:
    """Load parameter metadata

    Returns
    -------
        Parameter metadata in Polars LazyFrame
    """
    return pl.scan_parquet(Path(DATA_PATH, 'meta_parameters.parquet')).unique()


@st.cache_data
def load_meta_datainventory() -> pl.LazyFrame:
    """Load data inventory metadata

    Returns
    -------
        Data inventory metadata in Polars LazyFrame
    """
    return pl.scan_parquet(Path(DATA_PATH, 'meta_datainventory.parquet'))


@st.cache_data
def create_station_name_list(_frame_with_stations: pl.LazyFrame) -> Sequence[str]:
    return tuple(
        _frame_with_stations.unique(subset=['station_name'])
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
                [
                    'Short Code',
                    'Station Type',
                    'station_name',
                    'station_coordinates_wgs84_lat',
                    'station_coordinates_wgs84_lon',
                    'Altitude',
                ]
            )
        )
        .collect()
    )


meta_stations: pl.LazyFrame = load_meta_stations()
meta_parameters: pl.LazyFrame = load_meta_params()

regexp: Pattern[str] = re.compile(r'([\w\s()]+)')
rows: list[dict[str, Any]] = meta_parameters.collect().to_dicts()
meta_map: dict = {
    r['parameter_shortname']: re.search(regexp, r['parameter_description_en']).group()
    for r in rows
}
METRICS_NAMES_DICT: dict[str, str] = {m: meta_map.get(m, '') for m in METRICS_LIST}
WEATHER_COLUMN_NAMES_DICT: dict[str, str] = dict(
    {'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT
)
WEATHER_SHORT_LABEL_DICT: dict[str, str] = {
    'rre150h0': 'Precipitation',
    'tre200h0': 'Air Temperature',
    'ure200h0': 'Rel. Humidity',
    'fu3010h0': 'Wind Speed',
    'tde200h0': 'Dew Point',
}
df_weather: pl.LazyFrame = load_weather_data()
metrics: pl.LazyFrame = create_metrics(df_weather, TIME_PERIODS)
station_name_list: Sequence[str] = create_station_name_list(metrics)

st.title('MeteoFungi')

with st.sidebar:
    st.title('Stations')
    stations_options_selected = st.multiselect(
        label='Stations:',
        options=station_name_list,
        default='Airolo',
        max_selections=5,
        placeholder='Choose Station(s)',
    )
st.area_chart(
    data=(
        df_weather.sort('reference_timestamp')
        .filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo('Europe/Zurich'))
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
    ),
    x='Time',
    y='Precipitation',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)
station_frame_for_map: DataFrame = create_station_frame_for_map(meta_stations)
on: bool = st.toggle('Hide Map')
if not on:
    fig: Figure = px.scatter_map(
        station_frame_for_map,
        lat='station_coordinates_wgs84_lat',
        lon='station_coordinates_wgs84_lon',
        color='Station Type',
        hover_name='station_name',
        hover_data={
            'Station Type': False,
            'station_coordinates_wgs84_lat': False,
            'station_coordinates_wgs84_lon': False,
            'Short Code': True,
            'Altitude': True,
        },
        color_continuous_scale=px.colors.cyclical.IceFire,
        size_max=15,
        zoom=6,
        map_style='light',
        title='Station Locations',
    )
    st.plotly_chart(fig, use_container_width=True)


def create_metric_section(station_name: str, metrics_list: Sequence[str]):
    st.subheader(station_name)

    cols_metric: list[DeltaGenerator] = st.columns(len(metrics_list))
    for col, metric_name in zip(
        cols_metric,
        metrics_list,
        strict=False,
    ):
        val: float | None = calculate_metric_value(
            metric_name, station_name, number_days=NUM_DAYS_VAL
        )
        delta: float | None = calculate_metric_delta(
            metric_name, station_name, val, number_days=NUM_DAYS_DELTA
        )
        metric_label: str = WEATHER_SHORT_LABEL_DICT[metric_name]
        metric_tooltip: str = f'{WEATHER_COLUMN_NAMES_DICT[metric_name]} in {meta_parameters.filter(pl.col("parameter_shortname") == metric_name).select("parameter_unit").collect().item()}'
        metric_kwargs: dict[str, bool | str] = {
            'border': True,
            'help': metric_tooltip,
            'height': 'stretch',
        }
        if val is not None:
            col.metric(
                label=metric_label,
                value=(
                    str(round(val, 1))
                    + ' '
                    + (get_metric_emoji(val) if metric_name == 'rre150h0' else '')
                ),
                delta=str(round(delta, 1)),
                **metric_kwargs,
            )
        else:
            col.metric(label=metric_label, value='-', **metric_kwargs)


def calculate_metric_delta(
    metric_name: str, station_name: str, value: float | None, number_days: int
) -> float | None:
    if value is not None:
        if metric_name in PARAMETER_AGGREGATION_TYPES['sum']:
            return (
                value
                - filter_metrics_time_period(
                    station_name, number_days=number_days, metric_short_code=metric_name
                ).item()
            ) / number_days
        if metric_name in PARAMETER_AGGREGATION_TYPES['mean']:
            return (
                value
                - filter_metrics_time_period(
                    station_name, number_days=number_days, metric_short_code=metric_name
                ).item()
            )
        return None
    return None


def calculate_metric_value(
    metric_name: str, station_name: str, number_days: int
) -> float | None:
    if metric_name in PARAMETER_AGGREGATION_TYPES['sum']:
        return (
            calculate_metric_value_if_greater_zero(
                metric_name, station_name, number_days
            )
            / number_days
        )
    if metric_name in PARAMETER_AGGREGATION_TYPES['mean']:
        return calculate_metric_value_if_greater_zero(
            metric_name, station_name, number_days
        )
    return None


def calculate_metric_value_if_greater_zero(
    metric_name: str, station_name: str, number_days: int
) -> int:
    return (
        filter_metrics_time_period(
            station_name, number_days=number_days, metric_short_code=metric_name
        ).item()
        if (
            len(
                filter_metrics_time_period(
                    station_name, number_days=number_days, metric_short_code=metric_name
                )
            )
            > 0
        )
        else 0
    )


def filter_metrics_time_period(
    station_name: str, number_days: int, metric_short_code: str
) -> pl.DataFrame:
    return (
        metrics.filter(
            (pl.col('station_name') == station_name)
            & (pl.col('time_period') == number_days)
        )
        .select(pl.col(metric_short_code))
        .collect()
    )


for station in stations_options_selected:
    create_metric_section(station, METRICS_LIST)
create_metrics_expander_info(num_days_value=NUM_DAYS_VAL, num_days_delta=NUM_DAYS_DELTA)
