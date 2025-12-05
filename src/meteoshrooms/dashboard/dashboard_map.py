import logging
from typing import Any

import polars as pl
import streamlit as st
from plotly import express as px
from plotly.graph_objs import Figure

from meteoshrooms.dashboard.constants import WEATHER_SHORT_LABEL_DICT
from meteoshrooms.dashboard.dashboard_utils import (
    META_STATIONS,
    create_station_frame_for_map,
    update_selection,
)
from meteoshrooms.dashboard.log import init_logging

init_logging(__name__)
root_logger: logging.Logger = logging.getLogger(__name__)


def create_map_section(
    _metrics: pl.LazyFrame, param_short_code: str, time_period: int | None
):
    with st.container():
        fig: Figure = draw_map(_metrics, param_short_code, time_period)
        st.plotly_chart(
            fig,
            width='stretch',
            key='stations_selected_map',
            on_select=update_selection,
        )

        root_logger.debug('map created')


@st.cache_data
def draw_map(_metrics: pl.LazyFrame, param_short_code: str, time_period: int | None):
    if not time_period:
        time_period = 7
    station_frame_for_map: pl.DataFrame = create_station_frame_for_map(
        META_STATIONS, _metrics, time_period
    )
    scatter_map_kwargs: dict[
        str, str | dict[str, bool] | list[str | Any] | int | None
    ] = {
        'lat': 'station_coordinates_wgs84_lat',
        'lon': 'station_coordinates_wgs84_lon',
        'color': (WEATHER_SHORT_LABEL_DICT.get(param_short_code, 'Station Type')),
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
        'map_style': 'carto-positron',
        'title': (WEATHER_SHORT_LABEL_DICT.get(param_short_code, 'Stations')),
        'subtitle': (
            f'Over the last {time_period} days'
            if param_short_code in WEATHER_SHORT_LABEL_DICT
            else None
        ),
    }
    return px.scatter_map(station_frame_for_map, **scatter_map_kwargs)
