# Airflow Movie Pipeline

This repository contains an Airflow pipeline designed to process and analyze the **MovieLens 100K dataset**. The pipeline performs various tasks including calculating user statistics, identifying top-rated movies, analyzing genres by age groups, and finding similar movies based on user ratings.

## Project Overview

The **Airflow Movie Pipeline** processes movie data and user ratings from the MovieLens 100K dataset. The pipeline includes the following tasks:

1. **Find the Mean Age of Users in Each Occupation**:
   This task calculates the mean age of users for each occupation category using the user demographic information from the dataset.

2. **Find the Names of the Top 20 Highest-Rated Movies**:
   This task identifies the top 20 highest-rated movies that have received at least 35 ratings. It filters the movies based on user ratings and retrieves their titles.

3. **Find the Top Genres Rated by Users of Each Occupation in Age Groups**:
   This task analyzes the top genres rated by users, grouped by occupation and age ranges (20-25, 25-35, 35-45, and 45 and older).

4. **Find the Top 10 Similar Movies for a Given Movie**:
   Using user ratings, this task finds the top 10 movies most similar to a given movie based on rating patterns. The similarity can be calculated using methods like correlation.

## Dataset

The project uses the **MovieLens 100K dataset** from GroupLens. You can download the dataset from [MovieLens 100K](https://grouplens.org/datasets/movielens/100k/).

The dataset includes the following files:
- `u.data`: User ratings for movies.
- `u.item`: Movie metadata (titles, genres, release dates, etc.).
- `u.user`: User demographic information (age, gender, occupation, etc.).

These files are located in the `data/` folder of the project.

## File Structure
Workspace2/ 
├── dags/ 
│ └── movie_pipeline_dag.py # The Airflow DAG with the four tasks 
├── config/ 
│ └── config.yml # Configuration file for the pipeline 
├── data/ 
│ ├── u.data # User ratings 
│ ├── u.item # Movie metadata 
│ └── u.user # User demographic information 
├── logs/ # Airflow log files 
├── plugins/ # Any custom plugins (optional) 
└── scripts/ 
└── setup.sh # Bash script to set up the Airflow environment


## How to Run

### 1. Set Up Airflow Environment

Use the `setup.sh` script to initialize Airflow:

```bash
bash scripts/setup.sh

This will start the Airflow webserver and scheduler. Access the Airflow UI at http://localhost:8081.
2. Trigger the DAG

Once the Airflow environment is set up, trigger the movie_pipeline_dag from the Airflow UI or CLI.
3. Monitor the Pipeline

Monitor the status of the DAG and tasks through the Airflow UI. You can view logs and check task success/failure from the interface.

