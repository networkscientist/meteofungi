import urllib.request
import pandas as pd

# --- Load data ---
stations_rainfall = ['bey', 'mgl', 'sai']


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


def resample_time_data(df_to_resample):
    df_to_resample.reference_timestamp = pd.to_datetime(df_to_resample.reference_timestamp, format='%d.%m.%Y %H:%M')
    df_to_resample = df_to_resample.set_index(pd.DatetimeIndex(df_to_resample.reference_timestamp), drop=True).drop(
        columns='reference_timestamp'
    )
    df_to_resample = df_to_resample.loc[(df_to_resample.index.max() - pd.tseries.offsets.Day(7)) :]
    time_key = pd.Grouper(freq='6H')
    df_to_resample = df_to_resample.groupby(['station_abbr', time_key]).sum().reset_index(level=['station_abbr'])
    return df_to_resample


def load_rainfall(stations, metadata, from_local=False):
    rainfall = pd.DataFrame(index=pd.to_datetime([]))
    for station in stations:
        if from_local:
            df = pd.read_csv(f'{station}.csv', encoding='ISO-8859-1', sep=';', parse_dates=['reference_timestamp'])
        else:
            df = pd.read_csv(
                f'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn-precip/{station}/ogd-smn-precip_{station}_t_recent.csv',
                encoding='ISO-8859-1',
                sep=';',
                parse_dates=['reference_timestamp'],
            )
        df = resample_time_data(df)
        rainfall = pd.concat([rainfall, df])
    rainfall = rainfall.rename(columns={'station_abbr': 'Station', 'rre150z0': 'Rainfall'})
    rainfall = rainfall.replace(
        rainfall.Station.unique(),
        metadata.loc[metadata.station_abbr.str.lower().isin(stations), 'station_name'],
    )
    return rainfall


if __name__ == '__main__':
    meta = load_metadata()
    rainfall = load_rainfall(stations_rainfall, meta['precipitation'])
    rainfall.to_parquet('rainfall.parquet')
    