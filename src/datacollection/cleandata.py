import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

def load_data(engine):
    try:
        races = pd.read_sql("SELECT * FROM races", engine)
        results = pd.read_sql("SELECT * FROM results", engine)
        driver_standings = pd.read_sql("SELECT * FROM driver_standings", engine)
        
        try:
            qualifying = pd.read_sql("SELECT * FROM qualifying", engine)
        except:
            qualifying = None
            
        if races.empty or results.empty:
            raise ValueError("Required race or results data is missing")
        
        return races, results, driver_standings, qualifying
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise


def clean_races(races_df):

    races_df['date'] = pd.to_datetime(races_df['date'])
    races_df['circuit_id'] = races_df['circuit_id'].str.lower().str.replace(' ', '_')
    
    return races_df

def clean_results(results_df):
    
    results_df['position'] = pd.to_numeric(results_df['position'], errors='coerce')  
    results_df['points'] = results_df['points'].fillna(0)  
    results_df['grid'] = pd.to_numeric(results_df['grid'], errors='coerce')
    results_df['grid'] = results_df['grid'].fillna(results_df['grid'].mean())  
    
    status_mapping = {
        'Finished': 'Finished',
        'Did not finish': 'DNF',
        'Disqualified': 'DSQ',
        'Did not qualify': 'DNQ',
        'Accident': 'DNF',
        'Collision': 'DNF',
        'Engine': 'DNF-Mechanical'
    }
    
    results_df['status'] = results_df['status'].replace(status_mapping)
    results_df['date_of_birth'] = pd.to_datetime(results_df['date_of_birth'])
    
    return results_df

def clean_qualifying(qualifying_df):
    
    def convert_time_to_seconds(time_str):
        if pd.isna(time_str):
            return np.nan
        try:
            if ':' in time_str:
                minutes, rest = time_str.split(':')
                seconds = float(minutes) * 60 + float(rest)
            else:
                seconds = float(time_str)
            return seconds
        except:
            return np.nan
    
    qualifying_df['qualifying_time'] = qualifying_df['qualifying_time'].apply(convert_time_to_seconds)
    return qualifying_df

def aggregate_driver_data(results_df, races_df):
    
    merged_df = results_df.merge(races_df[['id', 'date', 'season']], 
                               left_on='race_id', 
                               right_on='id')
    
    driver_aggregates = merged_df.groupby(['driver', 'season']).agg(
        total_points=('points', 'sum'),
        total_wins=('position', lambda x: (x == 1).sum()),
        podiums=('position', lambda x: ((x >= 1) & (x <= 3)).sum()),
        avg_grid_position=('grid', 'mean'),
        avg_final_position=('position', 'mean'),
        total_races=('race_id', 'count'),
        dnf_races=('status', lambda x: (x == 'DNF').sum()),
        mechanical_failures=('status', lambda x: (x == 'DNF-Mechanical').sum()),
        consistency_score=('points', lambda x: x.std()),  
        qualifying_performance=('grid', lambda x: (x <= 3).sum()),  
        comeback_drives=('points', lambda x: ((x > 0) & (merged_df.loc[x.index, 'grid'] > 10)).sum()),
        points_per_race=('points', 'mean'),
        finish_rate=('status', lambda x: (x == 'Finished').mean())
    ).reset_index()
    
    driver_aggregates['finish_rate'] = 1 - (driver_aggregates['dnf_races'] / 
                                          driver_aggregates['total_races'])
    
    return driver_aggregates

def aggregate_constructor_data(results_df, races_df):
    
    merged_df = results_df.merge(races_df[['id', 'date', 'season']], 
                               left_on='race_id', 
                               right_on='id')

    constructor_aggregates = merged_df.groupby(['constructor', 'season']).agg(
        constructor_points=('points', 'sum'),
        avg_constructor_grid=('grid', 'mean'),
        avg_constructor_position=('position', 'mean'),
        total_constructor_races=('race_id', 'count')
    ).reset_index()
    return constructor_aggregates

def add_target_variable(driver_aggregates, driver_standings_df):
    
    try:
        if driver_standings_df['season'].dtype == 'object':
            driver_standings_df['season'] = driver_standings_df['season'].apply(
                lambda x: int(x) if isinstance(x, (int, str)) else int.from_bytes(x, byteorder='little')
            )
    except Exception as e:
        print(f"Error converting driver_standings seasons: {str(e)}")
        raise

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

def save_cleaned_data(driver_data, constructor_data, qualifying_data, engine):

    driver_data.to_sql("driver_aggregates", engine, if_exists="replace", index=False)
    constructor_data.to_sql("constructor_aggregates", engine, if_exists="replace", index=False)
    qualifying_data.to_sql("cleaned_qualifying", engine, if_exists="replace", index=False)

def clean_and_aggregate_data(database_path):

    engine = create_engine(f'sqlite:///{database_path}')
    session = sessionmaker(bind=engine)()

    try:
        print("Loading raw data...")
        races, results, driver_standings, qualifying = load_data(engine)
        
        print("Cleaning individual datasets...")
        try:
            races = clean_races(races)
            results = clean_results(results)
            
            if qualifying is not None and not qualifying.empty:
                qualifying = clean_qualifying(qualifying)
            else:
                qualifying = pd.DataFrame()  # Create empty DataFrame
        
        except Exception as e:
            print(f"Error during data cleaning: {str(e)}")
            raise
            
        print("Creating aggregations...")
        try:
            driver_aggregates = aggregate_driver_data(results, races)
            constructor_aggregates = aggregate_constructor_data(results, races)
            driver_aggregates = add_target_variable(driver_aggregates, driver_standings)
            
        except Exception as e:
            print(f"Error during aggregation: {str(e)}")
            raise
        
        print("Saving cleaned data...")
        try:
            save_cleaned_data(driver_aggregates, constructor_aggregates, 
                            qualifying, engine)
            print("All cleaned data saved")
            
        except Exception as e:
            print(f"Error saving cleaned data: {str(e)}")
            raise
        
        print("Data cleaning and aggregation completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error during cleaning process: {str(e)}")
        session.rollback()
        return False
    finally:
        session.close()

if __name__ == "__main__":
    database_path = "f1_prediction.db"
    clean_and_aggregate_data(database_path)