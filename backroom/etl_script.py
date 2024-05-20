import datetime
import pandas as pd
from unidecode import unidecode

from airflow.decorators import dag, task
from airflow.datasets import Dataset


IMDB_DATASET_PATH = './data/IMDbMovies-Clean.csv'
TMDB_DATASET_PATH = './data/TMDB_movie_dataset_v11.csv'


@dag(dag_id="MOVIES_ETL",
     description="Movies data ETL process for a datawarehouse project",
     start_date=datetime.datetime(2025, 5, 1),
     tags=['Movies'],
     dag_display_name='Movies_ETC',
     schedule=None)
def etl():
    @task
    def extract_IMDB():
        # extract data from the IMDB dataset
        columns = ['Title', 'Summary', 'Director', 'Main Genres', 'Motion Picture Rating',
                   'Runtime (Minutes)', 'Rating (Out of 10)', 'Number of Ratings (in thousands)',
                   'Budget (in millions)', 'Gross in US & Canada (in millions)', 'Gross worldwide (in millions)',
                   'Opening Weekend in US & Canada', 'Gross Opening Weekend (in millions)']

        df = pd.read_csv(IMDB_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        return df

    @task
    def extract_TMDB():
        # extract data from the TMDB dataset
        columns = ['title', 'release_date', 'original_language',
                   'production_countries', 'imdb_id']

        df = pd.read_csv(TMDB_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        return df

    @task
    def extract_actors():
        # extract data from the TMDB_celebs dataset
        pass

    @task
    def extract_directors():
        # extract data from the TMDB_celebs dataset
        pass

    @task
    def transform_movies():
        # clean data from movies. Should create a function for each dataset?
        pass

    @task
    def merge_movies():
        # merge movies datasets
        pass

    @task
    def transform_directors():
        # clean data from directors
        pass

    @task
    def merge_directors():
        # merge directors with movies
        pass

    @task
    def transform_actors():
        # clean data from actors
        pass

    @task
    def merge_actors():
        # merge actors with movies
        pass

    @task
    def load_data():
        # write data out to a single csv file
        pass
