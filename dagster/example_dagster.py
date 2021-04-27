from dagster import pipeline, solid
import pandas as pd


@solid
def read_filter_csv(context, file_location) -> str:
    df = pd.read_csv(file_location)
    filtered_df = df.filter(df['rideable_type'] == 'docked_bike', axis=0)
    filtered_df.to_csv('sample_data/filtered_data.csv')
    return 'sample_data/filtered_data.csv'


@solid
def calculate_metrics(context, data_file) -> None:
    df = pd.read_csv(data_file)
    df = df.groupby(['start_station_id']).count()
    df.to_csv('metrics.csv')


@pipeline
def first_pipeline():
    f = read_filter_csv()
    calculate_metrics(f)