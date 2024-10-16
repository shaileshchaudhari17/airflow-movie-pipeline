
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

# Dataset paths for the ml-100k dataset
MMOVIE_DATASET_PATH = '/path/to/Workspace2/data/u.item'
RATINGS_DATASET_PATH = '/path/to/Workspace2/data/u.data'
USER_DATASET_PATH = '/path/to/Workspace2/data/u.user'  

# Task 1: Find the mean age of users in each occupation (requires u.user file)
def calculate_mean_age():
    users_df = pd.read_csv(USER_DATASET_PATH, sep='|', names=['userId', 'age', 'gender', 'occupation', 'zip_code'])
    mean_age = users_df.groupby('occupation')['age'].mean()
    print(mean_age)
    return mean_age

# Task 2: Find the names of top 20 highest-rated movies (at least 35 ratings)
def find_top_movies():
    ratings_df = pd.read_csv(RATINGS_DATASET_PATH, sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])
    movies_df = pd.read_csv(MOVIE_DATASET_PATH, sep='|', encoding='ISO-8859-1', header=None, names=[
        'movieId', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure',
        'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical',
        'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
    ])

    # Filter movies with at least 35 ratings
    movie_ratings = ratings_df.groupby('movieId').filter(lambda x: len(x) >= 35)
    top_movies = movie_ratings.groupby('movieId').mean().sort_values(by='rating', ascending=False).head(20)

    # Merge with movie titles
    top_movies_with_names = pd.merge(top_movies, movies_df[['movieId', 'movie_title']], on='movieId')
    print(top_movies_with_names[['movie_title', 'rating']])
    return top_movies_with_names[['movie_title', 'rating']]

# Task 3: Find top genres rated by users of each occupation in age groups
def find_top_genres_by_age_group():
    ratings_df = pd.read_csv(RATINGS_DATASET_PATH, sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])
    movies_df = pd.read_csv(MOVIE_DATASET_PATH, sep='|', encoding='ISO-8859-1', header=None, names=[
        'movieId', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure',
        'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical',
        'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
    ])

    users_df = pd.read_csv(USER_DATASET_PATH, sep='|', names=['userId', 'age', 'gender', 'occupation', 'zip_code'])
    merged_df = pd.merge(ratings_df, users_df, on='userId')

    # Define age groups
    age_groups = [(20, 25), (25, 35), (35, 45), (45, 100)]

    # For each occupation and age group, find top genres
    for occupation, group in merged_df.groupby('occupation'):
        print(f"Occupation: {occupation}")
        for (low, high) in age_groups:
            age_group = group[(group['age'] >= low) & (group['age'] < high)]
            top_genres = age_group[movies_df.columns[6:]].sum().sort_values(ascending=False).head(3)  # Sum genre columns
            print(f"Age Group: {low}-{high}, Top Genres: {top_genres}")
            print()

# Task 4: Given one movie, find top 10 similar movies based on user ratings
def find_similar_movies(movie_id, similarity_threshold=0.9):
    ratings_df = pd.read_csv(RATINGS_DATASET_PATH, sep='\t', names=['userId', 'movieId', 'rating', 'timestamp'])

    # Create a user-movie matrix
    user_movie_matrix = ratings_df.pivot(index='userId', columns='movieId', values='rating').fillna(0)

    # Get the ratings for the given movie
    movie_ratings = user_movie_matrix[movie_id]

    # Calculate similarity (using correlation or cosine similarity as an example)
    movie_similarity = user_movie_matrix.corrwith(movie_ratings)

    # Filter movies based on similarity threshold and return top 10
    similar_movies = movie_similarity.sort_values(ascending=False).head(10)
    similar_movies = similar_movies[similar_movies >= similarity_threshold]

    print(f"Top 10 similar movies to movie ID {movie_id}:")
    print(similar_movies)
    return similar_movies

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'movie_pipeline_dag',
    default_args=default_args,
    description='Movie pipeline processing DAG',
    schedule_interval='0 20 * * 1-5',  # Run at 8 PM on every working day
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Calculate mean age of users in each occupation
    task_calculate_mean_age = PythonOperator(
        task_id='calculate_mean_age',
        python_callable=calculate_mean_age,
    )

    # Task 2: Find top 20 highest-rated movies
    task_find_top_movies = PythonOperator(
        task_id='find_top_movies',
        python_callable=find_top_movies,
    )

    # Task 3: Find top genres rated by users in each occupation and age group
    task_find_top_genres_by_age_group = PythonOperator(
        task_id='find_top_genres_by_age_group',
        python_callable=find_top_genres_by_age_group,
    )

    # Task 4: Find top 10 similar movies for a given movie
    task_find_similar_movies = PythonOperator(
        task_id='find_similar_movies',
        python_callable=lambda: find_similar_movies(movie_id=1),  # Example movie ID
    )

    # Task dependencies
    task_calculate_mean_age >> task_find_top_movies >> task_find_top_genres_by_age_group >> task_find_similar_movies
