import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

def load_data(engine):
    """
    Load raw data from SQLite database into pandas DataFrames.
    """
    races = pd.read_sql("SELECT * FROM races", engine)
    results = pd.read_sql("SELECT * FROM results", engine)
    driver_standings = pd.read_sql("SELECT * FROM driver_standings", engine)
    constructor_standings = pd.read_sql("SELECT * FROM constructor_standings", engine)
    return races, results, driver_standings, constructor_standings

def clean_results(results_df):
    """
    Clean the results DataFrame.
    """
    results_df['position'] = pd.to_numeric(results_df['position'], errors='coerce')  # Handle non-numeric positions
    results_df['points'] = results_df['points'].fillna(0)  # Fill missing points with 0
    results_df['grid'] = results_df['grid'].fillna(results_df['grid'].mean())  # Fill missing grid positions with mean
    return results_df

def aggregate_driver_data(results_df):
    """
    Aggregate driver-level data for each season.
    """
    driver_aggregates = results_df.groupby(['driver', 'season']).agg(
        total_points=('points', 'sum'),
        total_wins=('position', lambda x: (x == 1).sum()),
        avg_grid_position=('grid', 'mean'),
        avg_final_position=('position', 'mean'),
        total_races=('race_id', 'count')
    ).reset_index()
    return driver_aggregates

def aggregate_constructor_data(results_df):
    """
    Aggregate constructor-level data for each season.
    """
    constructor_aggregates = results_df.groupby(['constructor', 'season']).agg(
        constructor_points=('points', 'sum'),
        avg_constructor_grid=('grid', 'mean'),
        avg_constructor_position=('position', 'mean'),
        total_constructor_races=('race_id', 'count')
    ).reset_index()
    return constructor_aggregates

def add_target_variable(driver_aggregates, driver_standings_df):
    """
    Add the target variable (is_champion) to the driver aggregates.
    """
    champions = driver_standings_df[driver_standings_df['position'] == 1][['season', 'driver']]
    champions['is_champion'] = 1

    driver_aggregates = pd.merge(
        driver_aggregates,
        champions,
        how='left',
        on=['season', 'driver']
    )
    driver_aggregates['is_champion'] = driver_aggregates['is_champion'].fillna(0)
    return driver_aggregates

def save_cleaned_data(driver_data, constructor_data, engine):
    """
    Save the cleaned and aggregated data back to the SQLite database.
    """
    driver_data.to_sql("driver_aggregates", engine, if_exists="replace", index=False)
    constructor_data.to_sql("constructor_aggregates", engine, if_exists="replace", index=False)

def clean_and_aggregate_data(database_path):
    """
    Main function to clean and aggregate data.
    """
    # Connect to the database
    engine = create_engine(f'sqlite:///{database_path}')
    session = sessionmaker(bind=engine)()

    try:
        # Load raw data
        races, results, driver_standings, constructor_standings = load_data(engine)

        # Clean data
        results = clean_results(results)

        # Aggregate data
        driver_aggregates = aggregate_driver_data(results)
        constructor_aggregates = aggregate_constructor_data(results)

        # Add target variable
        driver_aggregates = add_target_variable(driver_aggregates, driver_standings)

        # Save cleaned data
        save_cleaned_data(driver_aggregates, constructor_aggregates, engine)

        print("Data cleaning and aggregation complete!")
    except Exception as e:
        print(f"Error during cleaning process: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    database_path = "f1_prediction.db"
    clean_and_aggregate_data(database_path)