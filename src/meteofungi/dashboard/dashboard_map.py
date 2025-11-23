from typing import Any

import polars as pl
import streamlit as st
from plotly import express as px
from plotly.graph_objs import Figure

from meteofungi.dashboard.constants import WEATHER_SHORT_LABEL_DICT
from meteofungi.dashboard.dashboard_utils import (
    META_STATIONS,
    create_station_frame_for_map,
)


def draw_map(metrics: pl.LazyFrame, param_short_code: str, time_period: int):
    if not time_period:
        time_period = 7
    station_frame_for_map: pl.LazyFrame = create_station_frame_for_map(
        META_STATIONS, metrics, time_period
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
    fig: Figure = px.scatter_map(station_frame_for_map.collect(), **scatter_map_kwargs)
    st.plotly_chart(fig, use_container_width=True)
