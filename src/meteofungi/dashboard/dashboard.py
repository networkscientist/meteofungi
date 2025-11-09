import logging
from logging import Logger

import polars as pl
import streamlit as st
from streamlit.logger import get_logger

from meteofungi.dashboard.constants import (
    METRICS_STRINGS,
    NUM_DAYS_DELTA,
    NUM_DAYS_VAL,
    TIME_PERIODS,
)
from meteofungi.dashboard.dashboard_map import draw_map
from meteofungi.dashboard.dashboard_timeseries_chart import create_area_chart
from meteofungi.dashboard.dashboard_utils import (
    META_STATIONS,
    create_metrics,
    create_station_frame_for_map,
    create_station_name_list,
    create_stations_options_selected,
    load_weather_data,
)
from meteofungi.dashboard.ux_metrics import (
    create_metric_section,
    create_metrics_expander_info,
)

logger: Logger = get_logger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug('Logger created')
# --- Load data ---
st.set_page_config(layout='wide', initial_sidebar_state='expanded')
logger.debug('Page config set')
df_weather: pl.LazyFrame = load_weather_data()
logger.debug('Weather data LazyFrame loaded')
metrics: pl.LazyFrame = create_metrics(df_weather, TIME_PERIODS)
logger.debug('Metrics LazyFrame created')
station_name_list: tuple[str, ...] = create_station_name_list(metrics)

st.title('MeteoShrooms')

with st.sidebar:
    st.title('Stations')
    stations_options_selected = create_stations_options_selected(station_name_list)

with st.container():
    create_area_chart(df_weather, stations_options_selected)
    on: bool = st.toggle('Hide Map')

    if not on:
        draw_map(create_station_frame_for_map(META_STATIONS))

    for station in stations_options_selected:
        create_metric_section(metrics, station, METRICS_STRINGS)
    create_metrics_expander_info(
        num_days_value=NUM_DAYS_VAL, num_days_delta=NUM_DAYS_DELTA
    )
