import polars as pl


DTYPE_DICT = {
    'Integer': pl.Int32,
    'Float': pl.Float32,
}


def load_meta_stations():
    return pl.scan_parquet('meta_stations.parquet')


def load_meta_params():
    return pl.scan_parquet('meta_params.parquet')


schema_dict = {
    colname: DTYPE_DICT[dtp]
    for colname, dtp in zip(
        load_meta_params().select('parameter_shortname').collect().to_series(),
        load_meta_params().select('parameter_datatype').collect().to_series(),
    )
}


def generate_download_url(station, station_type):
    if station_type == 'rainfall':
        return (
            f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_h_recent.csv'
        )
    elif station_type == 'weather':
        return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/{station}/ogd-smn_{station}_h_recent.csv'


def load_weather(metadata):
    stations = (
        metadata.select('station_abbr', 'station_type_en')
        .with_columns(pl.col('station_abbr'), pl.col('station_type_en'))
        .unique('station_abbr')
        .sort('station_abbr')
        .collect()
    )
    rainfall_recent = pl.scan_csv(
        [
            generate_download_url(station, 'rainfall')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic precipitation stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        separator=';',
        try_parse_dates=True,
        schema_overrides=schema_dict,
    )
    rainfall_now = pl.scan_csv(
        [
            generate_download_url(station, 'rainfall').replace('_recent', '_now')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic precipitation stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        separator=';',
        try_parse_dates=True,
        schema_overrides=schema_dict,
    )
    weather_recent = pl.scan_csv(
        [
            generate_download_url(station, 'weather')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic weather stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        separator=';',
        try_parse_dates=True,
        schema_overrides=schema_dict,
    )
    weather_now = pl.scan_csv(
        [
            generate_download_url(station, 'weather').replace('_recent', '_now')
            for station in stations.filter(pl.col('station_type_en') == 'Automatic weather stations')
            .select('station_abbr')
            .to_series()
            .str.to_lowercase()
        ],
        separator=';',
        try_parse_dates=True,
        schema_overrides=schema_dict,
    )
    rainfall = pl.concat(
        [
            rainfall_recent,
            rainfall_now,
        ]
    )
    weather = pl.concat(
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
    meta = load_meta_stations()
    weather_data = load_weather(meta)
    weather_data.sink_parquet('weather_data.parquet')
