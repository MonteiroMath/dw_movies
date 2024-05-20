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
    
    @task(task_id='extract_imdb')
    def extract_IMDB() -> pd.DataFrame:
        # extract data from the IMDB dataset
        columns = ['Title', 'Summary', 'Director', 'Main Genres', 'Motion Picture Rating',
                   'Runtime (Minutes)', 'Rating (Out of 10)', 'Number of Ratings (in thousands)',
                   'Budget (in millions)', 'Gross in US & Canada (in millions)', 'Gross worldwide (in millions)',
                   'Opening Weekend in US & Canada', 'Gross Opening Weekend (in millions)']

        df = pd.read_csv(IMDB_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        df.rename(mapper=(lambda x: unidecode(
            x.replace(' ', '_').lower())), axis=1, inplace=True)

        return df

    @task(task_id='extract_tmdb')
    def extract_TMDB() -> pd.DataFrame:
        # extract data from the TMDB dataset
        columns = ['title', 'release_date', 'original_language',
                   'production_countries', 'imdb_id']

        df = pd.read_csv(TMDB_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        df.rename(mapper=(lambda x: unidecode(
            x.replace(' ', '_').lower())), axis=1, inplace=True)

        return df

    @task(task_id='extract_actors')
    def extract_actors():
        # extract data from the TMDB_celebs dataset
        pass

    @task(task_id='directors')
    def extract_directors():
        # extract data from the TMDB_celebs dataset
        pass

    @task(task_id='transform_imdb')
    def transform_IMDB_movies(dataframe=pd.DataFrame) -> pd.DataFrame:
        # clean data from IMDB movies.
        df = dataframe

        # split director strings and replicate rows when there is more than one director
        try:
            df['director'] = df['director'].str.split(',')
            df = df.explode('director')
        except:
            pass

        # split genre strings and replicate rows when there is more than one genre
        try:
            df['main_genres'] = df['main_genres'].str.split(',')
            df = df.explode('main_genres')
        except:
            pass

        return df

    @task(task_id='transform_tmdb')
    def transform_TMDB_movies(dataframe=pd.DataFrame) -> pd.DataFrame:
        # clean data from TMDB movies.

        df = dataframe

        # split release date
        try:
            df['release_date'] = df['release_date'].apply(
                lambda x: pd.to_datetime(x, dayfirst=True))
            df['release_day'] = df['release_date'].apply(lambda x: x.day)
            df['release_month'] = df['release_date'].apply(lambda x: x.month)
            df['release_year'] = df['release_date'].apply(lambda x: x.year)
        except:
            pass

         # split production country strings and replicate rows when there is more than one production country
        try:
            df['production_country'] = df['production_country'].str.split(',')
            df = df.explode('production_country')
        except:
            pass

        return df

    @task(task_id='merge_movies')
    def merge_movies():
        # merge movies datasets
        pass

    @task(task_id='transform_directors')
    def transform_directors():
        # clean data from directors
        pass

    @task(task_id='merge_directors')
    def merge_directors():
        # merge directors with movies
        pass

    @task(task_id='transform_Actors')
    def transform_actors():
        # clean data from actors
        pass

    @task(task_id='merge_actors')
    def merge_actors():
        # merge actors with movies
        pass

    @task(task_id='load_data')
    def load_data():
        # write data out to a single csv file
        pass
