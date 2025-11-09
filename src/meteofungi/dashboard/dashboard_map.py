from typing import Any

import streamlit as st
from plotly import express as px
from plotly.graph_objs import Figure

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


def draw_map(station_frame_for_map):
    fig: Figure = px.scatter_map(station_frame_for_map, **scatter_map_kwargs)
    st.plotly_chart(fig, use_container_width=True)
