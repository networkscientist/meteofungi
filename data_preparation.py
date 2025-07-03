import polars as pl
from datetime import datetime, timedelta

# --- Load data ---
# stations = {'bey': 'rainfall', 'mgl': 'rainfall', 'sai': 'rainfall', 'coy': 'weather', 'cha': 'weather'}

WEATHER_SCHEMA = {'station_abbr': pl.String, 'reference_timestamp': pl.Datetime, 'tre200h0': pl.Float32, 'tre200hn': pl.Float32, 'tre200hx': pl.Float32, 'tre005h0': pl.Float32, 'tre005hn': pl.Float32, 'ure200h0': pl.Float32, 'pva200h0': pl.Float32, 'tde200h0': pl.Float32, 'prestah0': pl.Float32, 'pp0qffh0': pl.Float32, 'pp0qnhh0': pl.Float32, 'ppz700h0': pl.Float32, 'ppz850h0': pl.Float32, 'fkl010h1': pl.Float32, 'dkl010h0': pl.Int16, 'fkl010h0': pl.Float32, 'fu3010h0': pl.Float32, 'fu3010h1': pl.Float32, 'fkl010h3': pl.Float32, 'fu3010h3': pl.Float32, 'wcc006h0': pl.Int16, 'fve010h0': pl.Float32, 'rre150h0': pl.Float32, 'htoauths': pl.Int16, 'gre000h0': pl.Int16, 'oli000h0': pl.Int16, 'olo000h0': pl.Int16, 'osr000h0': pl.Int16, 'ods000h0': pl.Int16, 'sre000h0': pl.Int16, 'erefaoh0': pl.Float32}

def load_metadata_stations():
    return pl.scan_parquet('meta_stations.parquet')

def load_metadata_params():
    return pl.scan_parquet('meta_parameters.parquet')


def generate_download_url(station, station_type):
    if station_type == 'rainfall':
        return (
            f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_h_recent.csv'
        )
    elif station_type == 'weather':
        return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station}/ogd-smn_{station}_h_recent.csv'


def load_weather(stations, metadata):
    rainfall_recent = pl.scan_csv(
        [
            generate_download_url(station, station_type)
            for station, station_type in stations.items()
            if station_type == 'rainfall'
        ],
        separator=';',
        # try_parse_dates=True,
        schema=WEATHER_SCHEMA
    )
    rainfall_now = pl.scan_csv(
        [
            generate_download_url(station, station_type).replace('_recent', '_now')
            for station, station_type in stations.items()
            if station_type == 'rainfall'
        ],
        separator=';',
        # try_parse_dates=True,
        schema=WEATHER_SCHEMA,
    )
    weather_recent = pl.scan_csv(
        [
            generate_download_url(station, station_type)
            for station, station_type in stations.items()
            if station_type == 'weather'
        ],
        separator=';',
        # try_parse_dates=True,
        schema = WEATHER_SCHEMA
    )
    weather_now = pl.scan_csv(
        [
            generate_download_url(station, station_type).replace('_recent', '_now')
            for station, station_type in stations.items()
            if station_type == 'weather'
        ],
        separator=';',
        # try_parse_dates=True,
        schema=WEATHER_SCHEMA,
    )
    rainfall = pl.concat(
        [
            rainfall_recent.filter(
                pl.col('reference_timestamp') < rainfall_now.select('reference_timestamp').min().collect().item()
            ),
            rainfall_now,
        ]
    )
    weather = pl.concat(
        [
            weather_recent.filter(
                pl.col('reference_timestamp') < weather_now.select('reference_timestamp').min().collect().item()
            ),
            weather_now,
        ],
    )
    return (
        pl.concat([rainfall, weather], how='diagonal')
        .sort('reference_timestamp')
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_abbr')
        .sum()
        .join(metadata.select(['station_abbr', 'station_name']), on=['station_abbr'])
    )


def create_metrics(df):
    time_periods = {period: (datetime.now() - timedelta(days=period)) for period in [3, 7, 14, 30]}
    return pl.concat(
        [
            df.filter(pl.col('reference_timestamp') >= datetime_period)
            .drop('reference_timestamp')
            .group_by(['station_abbr', 'station_name'])
            .sum()
            .with_columns(pl.lit(period).alias('aggr_period_days'))
            for period, datetime_period in time_periods.items()
        ]
    )


if __name__ == '__main__':
    meta_stations = load_metadata_stations()
    meta_parameters = load_metadata_params()
    # data_type_map = {'Float': 'pl.Float32', 'Integer': 'pl.Int16', 'String': 'pl.String'}
    # meta_parameters = meta_parameters.with_columns(
    #     datatypes=pl.Series(
    #         [data_type_map[dt] for dt in meta_parameters.select('parameter_datatype').to_series()]
    #     )
    # )
    # meta_parameters = meta_parameters.with_columns(
    #     datatypes_obj=pl.Series(
    #         [eval(dt) for dt in meta_parameters.select('datatypes').to_series()]
    #     )
    # )
    # dtps = [({'station_abbr':pl.String, 'reference_timestamp':pl.String}|{col:tp for col, tp in zip(meta_parameters.select('parameter_shortname').to_series(), meta_parameters.select('datatypes_obj').to_series())})[tp] for tp in pl.read_csv('https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/cou/ogd-smn-precip_cou_h_recent.csv', separator=';', encoding='ISO-8859-1').columns]
    # cols = pl.read_csv('https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/cou/ogd-smn-precip_cou_h_recent.csv', separator=';', encoding='ISO-8859-1').columns
    # sc = pl.Schema({col: tp for col, tp in zip(cols, dtps)})


    stations = {station_abbr.lower():'weather' for station_abbr in meta_stations.filter(pl.col('station_type_de') == 'Automatische Wetterstationen').select('station_abbr').collect().to_series().to_list()} | {station_abbr.lower():'rainfall' for station_abbr in meta_stations.filter(pl.col('station_type_de') == 'Automatische Niederschlagsstationen').select('station_abbr').collect().to_series().to_list()}
    rainfall = load_weather(stations, meta_stations)
    rainfall.sink_parquet('rainfall.parquet')
    metrics = create_metrics(rainfall)
    metrics.sink_parquet('metrics.parquet')
