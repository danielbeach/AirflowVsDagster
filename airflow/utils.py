import pandas as pd


def read_filter_csv(file_location: str = 'sample_data/202004-divvy-tripdata.csv') -> None:
    df = pd.read_csv(file_location)
    filtered_df = df.filter(df['rideable_type'] == 'docked_bike', axis=0)
    filtered_df.to_csv('sample_data/filtered_data.csv')


def calculate_metrics(data_file: str = 'sample_data/filtered_data.csv') -> None:
    df = pd.read_csv(data_file)
    df = df.groupby(['start_station_id']).count()
    df.to_csv('metrics.csv')