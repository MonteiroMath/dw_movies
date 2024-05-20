import datetime
import pandas as pd
from unidecode import unidecode

from airflow.decorators import dag, task
from airflow.datasets import Dataset


IMDB_DATASET_PATH = './data/IMDbMovies-Clean.csv'
TMDB_DATASET_PATH = './data/TMDB_movie_dataset_v11.csv'
DIRECTORS_DATASET_PATH = './data/directors.csv'


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

        # list of columns to mantain
        columns = ['Title', 'Summary', 'Director', 'Main Genres', 'Motion Picture Rating',
                   'Runtime (Minutes)', 'Rating (Out of 10)', 'Number of Ratings (in thousands)', 'Budget (in millions)',
                   'Gross in US & Canada (in millions)', 'Gross worldwide (in millions)', 'Gross Opening Weekend (in millions)', 'Release Year']

        df = pd.read_csv(IMDB_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        # rename some columns
        df.rename(columns={
            'Motion Picture Rating': 'parental_rating',
            'Runtime (Minutes)': 'runtime',
            'Rating (Out of 10)': 'rating',
            'Number of Ratings (in thousands)': 'rating_num',
            'Budget (in millions)': 'budget',
            'Gross in US & Canada (in millions)': 'gross_usca',
            'Gross worldwide (in millions)': 'gross_world',
            'Gross Opening Weekend (in millions)': 'gross_opening',
            'Release Year': 'release_year'
        }, inplace=True)

        # clean column names
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

        df.rename(
            columns={'production_countries': 'production_country'}, inplace=True)

        # clean column names
        df.rename(mapper=(lambda x: unidecode(
            x.replace(' ', '_').lower())), axis=1, inplace=True)

        df['release_date'] = df['release_date'].apply(
            lambda x: pd.to_datetime(x))
        df['release_year'] = df['release_date'].apply(lambda x: x.year)
        return df

    @task(task_id='extract_actors')
    def extract_actors():
        # extract data from the TMDB_celebs dataset
        pass

    @task(task_id='directors')
    def extract_directors():
        # extract data from the directors dataset
        columns = ['itemLabel', 'genderLabel', 'dateOfBirth',
                   'placeOfBirthLabel', 'dateOfDeath']

        df = pd.read_csv(DIRECTORS_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        # rename some columns
        df.rename(columns={
            'itemLabel': 'director',
            'genderLabel': 'gender',
            'dateOfBirth': 'director_birth_date',
            'placeOfBirthLabel': 'director_birth_place',
            'dateOfDeath': 'director_death_date',
        }, inplace=True)

        return df

    @task(task_id='merge_movies')
    def merge_movies(df1=pd.DataFrame, df2=pd.DataFrame) -> pd.DataFrame:
        # merge movies datasets
        imdb_df = df1
        tmdb_df = df2

        df = pd.merge(imdb_df, tmdb_df, on='title', how='left')

        return df

    @task(task_id='transform_movies')
    def transform_movies(dataframe=pd.DataFrame) -> pd.DataFrame:
        # clean data from movies.
        df = dataframe

        # split release date

        df['release_date'] = df['release_date'].fillna(
            pd.to_datetime('1900-01-01'))

        df['release_date'] = df['release_date'].apply(
            lambda x: pd.to_datetime(x))
        df['release_day'] = df['release_date'].dt.day.astype('Int64')
        df['release_month'] = df['release_date'].dt.month.astype('Int64')
        df['release_year'] = df['release_date'].dt.year.astype('Int64')

        df[['release_day', 'release_month', 'release_year']] = df[[
            'release_day', 'release_month', 'release_year']].replace({1900: np.nan})

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

            # split production country strings and replicate rows when there is more than one production country
        try:
            df['production_country'] = df['production_country'].str.split(',')
            df = df.explode('production_country')
        except:
            pass

        return df

    @task(task_id='transform_directors')
    def transform_directors(dataframe=pd.DataFrame) -> pd.DataFrame:
        # clean data from directors
        df = dataframe

        # switch birth_date for birth_year
        try:
            df['director_birth_year'] = df['director_birth_date'].apply(
                lambda x: pd.to_datetime(x).dt.year.astype(int))
        except:
            pass

        # switch death_date for death_year
        try:
            df['director_death_year'] = df['director_death_date'].apply(
                lambda x: pd.to_datetime(x).dt.year.astype(int))
        except:
            pass

        # drop birth_date and death_date
        df = df.drop(columns=['director_birth_date', 'director_death_date'])

        return df

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
