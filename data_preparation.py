import urllib.request
import pandas as pd

# --- Load data ---
stations_rainfall = ['cou', 'bey', 'abe', 'gad', 'neb']

def download_rainfall_locally(stations):
    for station in stations:
        urllib.request.urlretrieve(
            f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_t_recent.csv',
            f'{station}.csv',
        )


def load_metadata():
    metadata = {
        'precipitation': pd.read_csv('ogd-smn-precip_meta_stations.csv', encoding='ISO-8859-1', sep=';'),
        'weather': pd.read_csv('ogd-smn_meta_stations.csv', encoding='ISO-8859-1', sep=';'),
    }
    return metadata


def load_rainfall(stations, metadata, from_local=False):
    rainfall = load_precipitation_into_dataframe(from_local, stations)
    rainfall.index = pd.DatetimeIndex(pd.to_datetime(rainfall.reference_timestamp, format='%d.%m.%Y %H:%M'))
    rainfall = (
        rainfall.sort_index()
        .loc[(rainfall.index.max() - pd.tseries.offsets.Day(7)) :]
        .rename(columns={'rre150z0': stations[0]})
    )
    for station in stations[1:]:
        if from_local:
            df = pd.read_csv(f'{station}.csv', encoding='ISO-8859-1', sep=';', parse_dates=['reference_timestamp'])
        else:
            df = pd.read_csv(
                f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_t_recent.csv',
                encoding='ISO-8859-1',
                sep=';',
                parse_dates=['reference_timestamp'],
            )
        df.reference_timestamp = pd.to_datetime(df.reference_timestamp, format='%d.%m.%Y %H:%M')
        df.index = pd.DatetimeIndex(df.reference_timestamp)
        df = df.sort_index().loc[(df.index.max() - pd.tseries.offsets.Day(7)) :]
        rainfall = rainfall.join(df.rre150z0.rename(station))
    rainfall = rainfall.drop(columns=['station_abbr', 'reference_timestamp'])
    rainfall = rainfall.rename(
        columns={
            key: value
            for key, value in zip(
                rainfall.columns,
                metadata.loc[
                    metadata.station_abbr.str.lower().isin(rainfall.columns),
                    'station_name',
                ],
            )
        }
    )
    rainfall = rainfall.rename(
        columns={
            key: value
            for key, value in zip(
                rainfall.columns,
                metadata.loc[metadata.station_abbr.str.lower().isin(rainfall.columns), 'station_name'],
            )
        }
    )
    return rainfall


def load_precipitation_into_dataframe(from_local, stations):
    if from_local:
        rainfall = pd.read_csv(
            f'{stations[0]}.csv',
            encoding='ISO-8859-1',
            sep=';',
            parse_dates=['reference_timestamp'],
        )
    else:
        rainfall = pd.read_csv(
            f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{stations[0]}/ogd-smn-precip_{stations[0]}_t_recent.csv',
            encoding='ISO-8859-1',
            sep=';',
            parse_dates=['reference_timestamp'],
        )
    return rainfall


meta = load_metadata()
rainfall = load_rainfall(stations_rainfall, meta['precipitation']).resample('6H').sum()
rainfall.to_parquet('rainfall.parquet')
