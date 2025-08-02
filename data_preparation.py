import polars as pl

# --- Load data ---
stations = {
    'bey': 'rainfall',
    'mgl': 'rainfall',
    'sai': 'rainfall',
    'coy': 'weather',
    'cha': 'weather',
}


def load_metadata():
    return pl.scan_parquet('meta_stations.parquet')


def generate_download_url(station, station_type):
    if station_type == 'rainfall':
        return f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_h_recent.csv'
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
        try_parse_dates=True,
    )
    rainfall_now = pl.scan_csv(
        [
            generate_download_url(station, station_type).replace('_recent', '_now')
            for station, station_type in stations.items()
            if station_type == 'rainfall'
        ],
        separator=';',
        try_parse_dates=True,
        schema=rainfall_recent.schema,
    )
    weather_recent = pl.scan_csv(
        [
            generate_download_url(station, station_type)
            for station, station_type in stations.items()
            if station_type == 'weather'
        ],
        separator=';',
        try_parse_dates=True,
    )
    weather_now = pl.scan_csv(
        [
            generate_download_url(station, station_type).replace('_recent', '_now')
            for station, station_type in stations.items()
            if station_type == 'weather'
        ],
        separator=';',
        try_parse_dates=True,
        schema=weather_recent.schema,
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
        .sum()
        .join(metadata.select(['station_abbr', 'station_name']), on=['station_abbr'])
    )


if __name__ == '__main__':
    meta = load_metadata()
    rainfall = load_weather(stations, meta)
    rainfall.sink_parquet('rainfall.parquet')
