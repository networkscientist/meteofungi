import polars.selectors as cs
import polars as pl

from pathlib import Path

DTYPE_DICT = {'Integer': pl.Int16, 'Float': pl.Float32, 'String': pl.String}

METEO_CSV_ENCODING: str = 'ISO-8859-1'


def load_meta_stations() -> pl.LazyFrame:
    # if Path('meta_stations.parquet').exists():
    #     return pl.scan_parquet('meta_stations.parquet')
    cols_to_keep: list[str] = [
        'station_' + x
        for x in 'abbr name canton type_de type_en dataowner data_since height_masl height_barometer_masl coordinates_lv95_east coordinates_lv95_north coordinates_wgs84_lat coordinates_wgs84_lon'.split()
    ]
    schem = {
        'station_abbr': pl.String,
        'station_name': pl.String,
        'station_canton': pl.String,
        'station_wigos_id': pl.String,
        'station_type_de': pl.String,
        'station_type_fr': pl.String,
        'station_type_it': pl.String,
        'station_type_en': pl.String,
        'station_dataowner': pl.String,
        'station_data_since': pl.String,
        'station_height_masl': pl.Float64,
        'station_height_barometer_masl': pl.Float64,
        'station_coordinates_lv95_east': pl.Float64,
        'station_coordinates_lv95_north': pl.Float64,
        'station_coordinates_wgs84_lat': pl.Float64,
        'station_coordinates_wgs84_lon': pl.Float64,
        'station_exposition_de': pl.String,
        'station_exposition_fr': pl.String,
        'station_exposition_it': pl.String,
        'station_exposition_en': pl.String,
        'station_url_de': pl.String,
        'station_url_fr': pl.String,
        'station_url_it': pl.String,
        'station_url_en': pl.String,
    }
    stations: pl.DataFrame = pl.concat(
        [
            pl.read_csv(
                f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn{ogd_smn_prefix}/ogd-smn{meta_suffix}_meta_stations.csv',
                encoding=METEO_CSV_ENCODING,
                separator=';',
                schema=schem,
                columns=cols_to_keep,
            )
            for ogd_smn_prefix, meta_suffix in zip(['', '-precip', '-tower'], ['', '-precip', '-tower'])
        ]
    )
    stations.write_parquet(Path('data/meta_stations.parquet'))
    return stations.lazy()


def load_meta_params() -> pl.LazyFrame:
    schem = {
        'parameter_shortname': pl.String,
        'parameter_description_de': pl.String,
        'parameter_description_fr': pl.String,
        'parameter_description_it': pl.String,
        'parameter_description_en': pl.String,
        'parameter_group_de': pl.String,
        'parameter_group_fr': pl.String,
        'parameter_group_it': pl.String,
        'parameter_group_en': pl.String,
        'parameter_granularity': pl.String,
        'parameter_decimals': pl.Int8,
        'parameter_datatype': pl.String,
        'parameter_unit': pl.String,
    }
    params: pl.DataFrame = pl.concat(
        [
            pl.read_csv(
                f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn{ogd_smn_prefix}/ogd-smn{meta_suffix}_meta_parameters.csv',
                encoding=METEO_CSV_ENCODING,
                separator=';',
                schema=schem,
            )
            for ogd_smn_prefix, meta_suffix in zip(['', '-precip', '-tower'], ['', '-precip', '-tower'])
        ]
    ).drop([cs.ends_with('_fr'), cs.ends_with('_it')])
    params.write_parquet(Path('data/meta_parameters.parquet'))
    return params.lazy()


def generate_download_url(station: str, station_type: str) -> str:
    if station_type == 'rainfall':
        return (
            f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_h_recent.csv'
        )
    elif station_type == 'weather':
        return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station}/ogd-smn_{station}_h_recent.csv'
    else:
        raise TypeError('station_type must be String and cannot be None')


def load_weather(metadata: pl.LazyFrame, schema_dict_lazyframe: dict) -> pl.LazyFrame:
    stations: pl.DataFrame = (
        metadata.select('station_abbr', 'station_type_en')
        .with_columns(pl.col('station_abbr'), pl.col('station_type_en'))
        .unique('station_abbr')
        .sort('station_abbr')
        .collect()
    )
    kwargs_lazyframe = {'separator': ';', 'try_parse_dates': True, 'schema_overrides': schema_dict_lazyframe}
    rainfall_recent: pl.LazyFrame = pl.scan_csv(
        [
            generate_download_url(station, 'rainfall')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic precipitation stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        **kwargs_lazyframe,
    )
    rainfall_now: pl.LazyFrame = pl.scan_csv(
        [
            generate_download_url(station, 'rainfall').replace('_recent', '_now')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic precipitation stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        **kwargs_lazyframe,
    )
    weather_recent: pl.LazyFrame = pl.scan_csv(
        [
            generate_download_url(station, 'weather')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic weather stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        **kwargs_lazyframe,
    )
    weather_now: pl.LazyFrame = pl.scan_csv(
        [
            generate_download_url(station, 'weather').replace('_recent', '_now')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic weather stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        **kwargs_lazyframe,
    )
    rainfall: pl.LazyFrame = pl.concat(
        [
            rainfall_recent,
            rainfall_now,
        ]
    )
    weather: pl.LazyFrame = pl.concat(
        [
            weather_recent,
            weather_now,
        ],
    )
    return (
        pl.concat([rainfall, weather], how='diagonal')
        .sort('reference_timestamp')
        .group_by_dynamic('reference_timestamp', every='1h', group_by='station_abbr')
        .agg(pl.sum('rre150h0'), pl.mean('tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0'))
        .join(metadata.select(['station_abbr', 'station_name']), on=['station_abbr'])
        # .sort('reference_timestamp')
    )


if __name__ == '__main__':
    weather_schema_dict = {
        colname: DTYPE_DICT[datatype]
        for colname, datatype in load_meta_params()
        .select(pl.col('parameter_shortname'), pl.col('parameter_datatype'))
        .collect()
        .iter_rows()
    }

    meta_stations: pl.LazyFrame = load_meta_stations()
    weather_data: pl.LazyFrame = load_weather(meta_stations, schema_dict_lazyframe=weather_schema_dict)
    weather_data.sink_parquet(Path('data/weather_data.parquet'), compression='brotli', compression_level=11)
