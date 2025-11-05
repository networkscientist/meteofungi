from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st
from plotly.graph_objs import Figure
from polars import DataFrame

from meteofungi.constants import (
    DATA_PATH,
    TIMEZONE_SWITZERLAND_STRING,
)
from meteofungi.dashboard.constants import (
    METRICS_STRINGS,
    NUM_DAYS_DELTA,
    NUM_DAYS_VAL,
    PARAMETER_AGGREGATION_TYPES,
    TIME_PERIODS,
)
from meteofungi.dashboard.dashboard_utils import (
    META_STATIONS,
    create_area_chart_frame,
    create_stations_options_selected,
    scatter_map_kwargs,
)
from meteofungi.dashboard.ux_metrics import (
    create_metric_section,
    create_metrics_expander_info,
)

# --- Load data ---
st.set_page_config(layout='wide', initial_sidebar_state='expanded')


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
def load_weather_data() -> pl.LazyFrame:
    return pl.scan_parquet(Path(DATA_PATH, 'weather_data.parquet')).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            TIMEZONE_SWITZERLAND_STRING, non_existent='null'
        )
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


df_weather: pl.LazyFrame = load_weather_data()
metrics: pl.LazyFrame = create_metrics(df_weather, TIME_PERIODS)
station_name_list: tuple[str, ...] = create_station_name_list(metrics)

st.title('MeteoShrooms')

with st.sidebar:
    st.title('Stations')
    stations_options_selected = create_stations_options_selected(station_name_list)
st.area_chart(
    data=create_area_chart_frame(df_weather, stations_options_selected),
    x='Time',
    y='Precipitation',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)
station_frame_for_map: DataFrame = create_station_frame_for_map(META_STATIONS)
on: bool = st.toggle('Hide Map')
if not on:
    fig: Figure = px.scatter_map(station_frame_for_map, **scatter_map_kwargs)
    st.plotly_chart(fig, use_container_width=True)

for station in stations_options_selected:
    create_metric_section(metrics, station, METRICS_STRINGS)
create_metrics_expander_info(num_days_value=NUM_DAYS_VAL, num_days_delta=NUM_DAYS_DELTA)
