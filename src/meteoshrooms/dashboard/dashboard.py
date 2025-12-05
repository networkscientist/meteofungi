import importlib.metadata
import logging

import polars as pl
import streamlit as st

from meteoshrooms.dashboard.constants import (
    METRICS_STRINGS,
    NUM_DAYS_DELTA,
    NUM_DAYS_VAL,
    TIME_PERIODS,
)
from meteoshrooms.dashboard.dashboard_map import create_map_section
from meteoshrooms.dashboard.dashboard_timeseries_chart import create_area_chart
from meteoshrooms.dashboard.dashboard_utils import (
    create_station_names,
    create_stations_options_selected,
    load_metric_data,
    load_weather_data,
)
from meteoshrooms.dashboard.log import init_logging
from meteoshrooms.dashboard.ux_metrics import (
    create_metric_section,
    create_metrics_expander_info,
)


def main():
    if 'stations_options_multiselect' not in st.session_state:
        st.session_state.stations_options_multiselect = {'Airolo'}
    if 'stations_selected_last_time' not in st.session_state:
        st.session_state.stations_selected_last_time = {'Airolo'}
    st.set_page_config(layout='wide', initial_sidebar_state='expanded')
    root_logger.debug('Page config set')
    df_weather: pl.LazyFrame = load_weather_data().lazy()
    root_logger.debug('Weather data LazyFrame loaded')
    metrics: pl.LazyFrame = load_metric_data().lazy()
    root_logger.debug('Metrics LazyFrame created')
    station_name_list: tuple[str, ...] = create_station_names(metrics)
    st.title('MeteoShrooms')

    with st.sidebar:
        st.title('Selection')
        stations_options_selected: list = create_stations_options_selected(
            station_name_list
        )
        time_period_selected: int | None = st.pills(
            'Time Period', TIME_PERIODS.keys(), default=7
        )
        toggle_hide_map: bool = st.toggle('Hide Map')

    with st.container():
        create_area_chart(
            df_weather, stations_options_selected, time_period_selected, 'rre150h0'
        )
    if not toggle_hide_map:
        create_map_section(metrics, 'rre150h0', time_period_selected)
    with st.container():
        for station in stations_options_selected:
            create_metric_section(metrics, station, METRICS_STRINGS)
        create_metrics_expander_info(
            num_days_value=NUM_DAYS_VAL, num_days_delta=NUM_DAYS_DELTA
        )
    st.caption(f'MeteoShrooms Version: {importlib.metadata.version("MeteoShrooms")}')


if __name__ == '__main__':
    init_logging(__name__)
    root_logger: logging.Logger = logging.getLogger(__name__)
    root_logger.debug('Logger created')

    # cProfile.run("main()", sort='ncalls')
    # import cProfile
    # from pstats import Stats
    #
    # pr = cProfile.Profile()
    # pr.enable()

    main()

    # pr.disable()
    # stats = Stats(pr)
    # stats.sort_stats('tottime').print_stats(10)
