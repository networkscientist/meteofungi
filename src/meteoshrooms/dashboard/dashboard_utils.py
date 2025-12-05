import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
import streamlit as st

from meteoshrooms.constants import (
    DATA_PATH,
    TIMEZONE_SWITZERLAND_STRING,
    parameter_description_extraction_pattern,
)
from meteoshrooms.dashboard.constants import (
    COLUMNS_FOR_MAP_FRAME,
    METRICS_STRINGS,
    SIDEBAR_MAX_SELECTIONS,
    WEATHER_SHORT_LABEL_DICT,
)
from meteoshrooms.dashboard.log import init_logging

init_logging(__name__)
root_logger: logging.Logger = logging.getLogger(__name__)


@st.cache_data
def load_metadata_to_frame(meta_type: str) -> pl.DataFrame:
    """Load metadata

    Returns
    -------
        Metadata in Polars DataFrame
    """
    return pl.read_parquet(
        Path(DATA_PATH, f'meta_{meta_type.lower()}.parquet')
    ).unique()


@st.cache_data
def collect_meta_params_to_dicts(metadata: pl.DataFrame) -> tuple[dict[str, Any], ...]:
    return tuple(
        metadata.to_dicts(),
    )


@st.cache_data
def create_meta_map(metadata: pl.DataFrame):
    meta_map: dict = {
        r['parameter_shortname']: (
            result.group()
            if (
                result := re.search(
                    parameter_description_extraction_pattern,
                    r['parameter_description_en'],
                )
            )
            is not None
            else None
        )
        for r in collect_meta_params_to_dicts(metadata)
    }
    return meta_map


def create_stations_options_selected(station_name_list) -> list:
    return st.multiselect(
        label='Stations',
        options=station_name_list,
        default='Airolo',
        max_selections=SIDEBAR_MAX_SELECTIONS,
        placeholder='Choose Station(s)',
        key='stations_options_multiselect',
    )


@st.cache_data
def create_station_names(_frame_with_stations: pl.LazyFrame) -> tuple[str, ...]:
    return tuple(
        _frame_with_stations.unique(subset=('station_name',))
        .sort('station_name')
        .select('station_name')
        .collect()
        .to_series()
        .to_list()
    )


@st.cache_data
def create_station_frame_for_map(
    _frame_with_stations: pl.LazyFrame, _metrics: pl.LazyFrame, time_period: int
) -> pl.DataFrame:
    return (
        _frame_with_stations.with_columns(
            pl.col('station_type_en').alias('Station Type'),
            pl.col('station_abbr').alias('Short Code'),
            Altitude=pl.col('station_height_masl')
            .cast(pl.Int16)
            .cast(pl.String)
            .add(' m.a.s.l'),
        )
        .select(pl.col(COLUMNS_FOR_MAP_FRAME))
        .join(
            _metrics.filter(pl.col('time_period') == time_period),
            left_on='Short Code',
            right_on='station_abbr',
        )
        .rename(WEATHER_SHORT_LABEL_DICT)
        .collect()
    )


@st.cache_data
def load_weather_data() -> pl.DataFrame:
    return pl.read_parquet(Path(DATA_PATH, 'weather_data.parquet')).with_columns(
        pl.col('reference_timestamp').dt.replace_time_zone(
            TIMEZONE_SWITZERLAND_STRING, non_existent='null'
        )
    )


@st.cache_data
def load_metric_data() -> pl.DataFrame:
    return pl.read_parquet(Path(DATA_PATH, 'metrics.parquet')).pivot(
        'parameter',
        index=('station_abbr', 'station_name', 'time_period'),
        values='value',
    )


@st.cache_data
def create_metrics_names_dict(meta_params_df: pl.DataFrame) -> dict[str, str]:
    return {m: create_meta_map(meta_params_df).get(m, '') for m in METRICS_STRINGS}


META_PARAMETERS: pl.DataFrame = load_metadata_to_frame('parameters')

META_STATIONS: pl.LazyFrame = load_metadata_to_frame('stations').lazy()

METRICS_NAMES_DICT: dict[str, str] = create_metrics_names_dict(META_PARAMETERS)
WEATHER_COLUMN_NAMES_DICT: dict[str, str] = dict(
    {'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT
)


def update_selection():
    try:
        if len(st.session_state.stations_selected_map.selection.points) > 0:
            new_selection = {
                pt['hovertext']
                for pt in st.session_state.stations_selected_map.selection.points
            }
            st.session_state.stations_selected_last_time = (
                st.session_state.stations_options_multiselect
            )
            root_logger.debug(len(st.session_state.stations_options_multiselect))
            if len(st.session_state.stations_options_multiselect) == 5:
                raise IndexError(
                    'You have already selected the maximum number of stations (5).'
                )
            if any(
                pt not in st.session_state.stations_options_multiselect
                for pt in new_selection
            ):
                root_logger.debug(new_selection)
                st.session_state.stations_options_multiselect = sorted(
                    st.session_state.stations_options_multiselect
                    + list(
                        new_selection.difference(
                            set(st.session_state.stations_options_multiselect)
                        )
                    )
                )[0:5]
        else:  # Pass for the time being
            pass
    except Exception as e:
        st.error(e)
