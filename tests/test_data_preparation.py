"""Tests module meteofungi.data_preparation.data_preparation.py"""

from pathlib import Path

import polars as pl
import polars.selectors as cs
import pytest
from polars.testing import assert_frame_equal

from meteofungi.data_preparation.constants import (
    COLS_TO_KEEP_META_DATAINVENTORY,
    COLS_TO_KEEP_META_PARAMETERS,
    COLS_TO_KEEP_META_STATIONS,
    SCHEMA_META_DATAINVENTORY,
    SCHEMA_META_PARAMETERS,
    SCHEMA_META_STATIONS,
)
from meteofungi.data_preparation.data_preparation import load_metadata


@pytest.fixture(scope='session')
def test_data_path():
    data_path: Path = Path(__file__).resolve().parents[0].joinpath('data')
    return data_path


@pytest.fixture(scope='class')
def temporary_data_path(tmp_path_factory):
    return tmp_path_factory.mktemp('data')


@pytest.fixture(scope='session')
def meta_file_path_dict(test_data_path):
    """Creates Dictionary with local metadata file paths"""
    meta_file_path_dict: dict[str, list[str]] = {
        'stations': [
            str(
                Path(
                    test_data_path, f'ogd-smn{meta_suffix}_meta_stations_test_data.csv'
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
        'parameters': [
            str(
                Path(
                    test_data_path,
                    f'ogd-smn{meta_suffix}_meta_parameters_test_data.csv',
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
        'datainventory': [
            str(
                Path(
                    test_data_path,
                    f'ogd-smn{meta_suffix}_meta_datainventory_test_data.csv',
                )
            )
            for ogd_smn_prefix, meta_suffix in zip(
                ['', '-precip', '-tower'], ['', '-precip', '-tower'], strict=False
            )
        ],
    }
    return meta_file_path_dict


@pytest.fixture
def lf_meta_stations_test_result(test_data_path):
    """Loads stations test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_stations_test_result.csv'))
    )


@pytest.fixture
def lf_meta_parameters_test_result(test_data_path):
    """Loads parameters test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_parameters_test_result.csv'))
    ).cast({cs.integer(): pl.Int8})


@pytest.fixture
def lf_meta_datainventory_test_result(test_data_path):
    """Loads datainventory test result into LazyFrame"""
    return pl.scan_csv(
        str(Path(test_data_path, 'ogd-smn_meta_datainventory_test_result.csv'))
    ).cast({cs.integer(): pl.Int8, cs.starts_with('data_'): pl.Datetime})


@pytest.fixture(scope='class')
def attach_lf_meta_stations(request, meta_file_path_dict, temporary_data_path):
    """Returns meta_stations created by load_metadata()"""
    cls = request.node.cls
    cls.lf_meta_stations = load_metadata(
        meta_type='stations',
        file_path_dict=meta_file_path_dict,
        meta_schema=SCHEMA_META_STATIONS,
        meta_cols_to_keep=COLS_TO_KEEP_META_STATIONS,
        data_path=temporary_data_path,
    )
    yield
    del cls.lf_meta_stations


@pytest.fixture(scope='class')
def attach_lf_meta_parameters(request, meta_file_path_dict, temporary_data_path):
    """Returns meta_parameters created by load_metadata()"""
    cls = request.node.cls
    cls.lf_meta_parameters = load_metadata(
        meta_type='parameters',
        file_path_dict=meta_file_path_dict,
        meta_schema=SCHEMA_META_PARAMETERS,
        meta_cols_to_keep=COLS_TO_KEEP_META_PARAMETERS,
        data_path=temporary_data_path,
    )
    yield
    del cls.lf_meta_parameters


@pytest.fixture(scope='class')
def attach_lf_meta_datainventory(request, meta_file_path_dict, temporary_data_path):
    """Returns meta_datainventory created by load_metadata()"""
    cls = request.node.cls
    cls.lf_meta_datainventory = load_metadata(
        meta_type='datainventory',
        file_path_dict=meta_file_path_dict,
        meta_schema=SCHEMA_META_DATAINVENTORY,
        meta_cols_to_keep=COLS_TO_KEEP_META_DATAINVENTORY,
        data_path=temporary_data_path,
    )
    yield
    del cls.lf_meta_datainventory


@pytest.mark.usefixtures(
    'attach_lf_meta_stations',
    'attach_lf_meta_parameters',
    'attach_lf_meta_datainventory',
)
class TestLoadMetadata:
    """Tests function load_metadata()"""

    def test_load_metadata_stations_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.lf_meta_stations, pl.LazyFrame)

    def test_load_metadata_stations_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_stations_test_result
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(self.lf_meta_stations, lf_meta_stations_test_result)

    def test_load_metadata_parameters_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.lf_meta_parameters, pl.LazyFrame)

    def test_load_metadata_parameters_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_parameters_test_result
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(self.lf_meta_parameters, lf_meta_parameters_test_result)

    def test_load_metadata_datainventory_is_lazyframe(self):
        """Tests whether returned frame is LazyFrame"""
        assert isinstance(self.lf_meta_datainventory, pl.LazyFrame)

    def test_load_metadata_datainventory_assert_equal_lazyframes(
        self, meta_file_path_dict, lf_meta_datainventory_test_result
    ):
        """Tests if returned frame is equal to test data"""
        assert_frame_equal(
            self.lf_meta_datainventory, lf_meta_datainventory_test_result
        )
