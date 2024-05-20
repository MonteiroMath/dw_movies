import datetime
import pandas as pd
from unidecode import unidecode

from airflow.decorators import dag, task
from airflow.datasets import Dataset


@dag(dag_id="MOVIES_ETL",
     description="Movies data ETL process for a datawarehouse project",
     start_date=datetime.datetime(2025, 5, 1),
     tags=['Movies'],
     dag_display_name='Movies_ETC',
     schedule=None)
def etl():
    def extract_IMDB():
        # extract data from the IMDB dataset
        pass

    def extract_title_basics():
        # extract data from the tmdb dataset
        pass

    def extract_TMDB():
        # extract data from the TMDB dataset
        pass

    def extract_actors():
        # extract data from the TMDB_celebs dataset
        pass

    def extract_directors():
        # extract data from the TMDB_celebs dataset
        pass

    def transform_movies():
        # clean data from movies. Should create a function for each dataset?
        pass

    def merge_movies():
        # merge movies datasets
        pass

    def transform_directors():
        # clean data from directors
        pass

    def merge_directors():
        # merge directors with movies
        pass

    def transform_actors():
        # clean data from actors
        pass

    def merge_actors():
        # merge actors with movies
        pass

    def load_data():
        # write data out to a single csv file
        pass
