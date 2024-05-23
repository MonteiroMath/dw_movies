import datetime
import pandas as pd
from unidecode import unidecode
import numpy as np

from airflow.decorators import dag, task
from airflow.datasets import Dataset


IMDB_DATASET_PATH = './data/IMDbMovies-Clean.csv'
TMDB_DATASET_PATH = './data/TMDB_movie_dataset_v11.csv'
IMDB_ORIGINAL_PATH = './data/title.basics.tsv'
DIRECTORS_GENDER_DATASET_PATH = './data/directors.csv'
DIRECTORS_DATASET_PATH = './data/directorspermovies.csv'
ACTORS_DATASET_PATH = './data/actorspermovies.csv'


@dag(dag_id="MOVIES_ETL",
     description="Movies data ETL process for a datawarehouse project",
     start_date=datetime.datetime(2024, 5, 1),
     tags=['Movies'],
     dag_display_name='Movies_ETL',
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

        df = df[df['release_year'] > 2022]
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
    def extract_actors() -> pd.DataFrame:
        # extract data from the actors dataset

        columns = ['tconst', 'name', 'category', 'birth_year', 'death_year']
        df = pd.read_csv(ACTORS_DATASET_PATH, sep=';',
                         encoding='utf-8', usecols=columns)

        return df

    @task(task_id='extract_directors')
    def extract_directors() -> pd.DataFrame:
        # extract data from the directors dataset
        columns = ['tconst', 'name', 'birth_year', 'death_year']

        df = pd.read_csv(DIRECTORS_DATASET_PATH, sep=';',
                         encoding='utf-8', usecols=columns)

        return df

    @task(task_id='extract_director_gender')
    def extract_directors_gender() -> pd.DataFrame:
        # extract data from the directors gender dataset
        columns = ['itemLabel', 'genderLabel']

        df = pd.read_csv(DIRECTORS_GENDER_DATASET_PATH, sep=',',
                         encoding='utf-8', usecols=columns)

        # rename some columns
        df.rename(columns={
            'itemLabel': 'name',
            'genderLabel': 'gender',
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
            df['production_country'] = df['production_country'].str.strip()
        except:
            pass

        return df

    @task(task_id='transform_directors')
    def transform_directors(directorsDF=pd.DataFrame, genderDF=pd.DataFrame) -> pd.DataFrame:
        # clean data from directors
        df = pd.merge(directorsDF, genderDF, on='name', how='left')

        # add unique identifier to each row
        df['id'] = range(1, len(df) + 1)
        df['id'] = df['id'].astype(int)

        return df

    @task(task_id='merge_directors')
    def merge_directors(df1=pd.DataFrame, df2=pd.DataFrame) -> pd.DataFrame:
        # merge directors with movies
        directors_df = df1
        movies_df = df2

        df = pd.merge(movies_df, directors_df, left_on='imdb_id',
                      right_on='tconst', how='left')

        return df

    @task(task_id='transform_actors')
    def transform_actors(actorsDF=pd.DataFrame) -> pd.DataFrame:
        # clean data from actors
        df = actorsDF

        # add the gender column to actors
        df['gender'] = df['category'].map(
            {'actor': 'male', 'actress': 'female'})
        df = df.drop('category', axis=1)

        # add unique identifier to each row
        df['id'] = range(1, len(df) + 1)
        df['id'] = df['id'].astype(int)
        return df

    @task(task_id='merge_actors')
    def merge_actors(moviesDF=pd.DataFrame, actorsDF=pd.DataFrame) -> pd.DataFrame:
        # merge actors with movies

        df = pd.merge(moviesDF, actorsDF, left_on='imdb_id',
                      right_on='tconst', suffixes=['_director', '_actor'], how='left')
        return df

    @task(task_id='load_data')
    def load_data(df):
        # write data out to a single csv file
        df.to_csv('/home/matheus/Code/fac/current/dw/tf/output.csv',
                  index=False, sep=';')

    # extract and transform movies
    imdb = extract_IMDB()
    tmdb = extract_TMDB()
    movies = merge_movies(imdb, tmdb)
    movies = transform_movies(movies)

    # extract, transform directors
    directors = extract_directors()
    directors_gender = extract_directors_gender()
    directors_transformed = transform_directors(directors, directors_gender)
    movies_with_directors = merge_directors(directors_transformed, movies)

    # extract, transform actors
    actors = extract_actors()
    #actors = transform_actors(actors)
    movies_full = merge_actors(movies_with_directors, actors)
    load_data(movies_full)


etl()
