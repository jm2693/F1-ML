import pandas as pd
from database.database import Database
from src.datacollection.collect_data import (
    collect_race_data, 
    collect_driver_standings,
    collect_constructor_standings, 
    get_rounds
)

def test_database_setup():

    print("\nTesting database setup...")
    try:
        db = Database()
        db.create_tables()
        print("✓ Database setup successful")
        return True
    except Exception as e:
        print(f"✗ Database setup failed: {str(e)}")
        return False

def test_race_collection():

    print("\nTesting race data collection...")
    try:
        collect_race_data(start_year=2020, end_year=2020)
        
        db = Database()
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races WHERE season = 2020", session.bind)
        results_df = pd.read_sql("SELECT * FROM results WHERE race_id IN (SELECT id FROM races WHERE season = 2020)", session.bind)
        
        print(f"✓ Successfully collected {len(races_df)} races and {len(results_df)} results from 2020")
        return True
    except Exception as e:
        print(f"✗ Race collection failed: {str(e)}")
        return False

def test_standings_collection():

    print("\nTesting standings collection...")
    try:
        db = Database()
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races", session.bind)
        rounds = get_rounds(races_df)
        
        collect_driver_standings(rounds)
        collect_constructor_standings(rounds)
        
        driver_standings_df = pd.read_sql("SELECT * FROM driver_standings WHERE season = 2020", session.bind)
        constructor_standings_df = pd.read_sql("SELECT * FROM constructor_standings WHERE season = 2020", session.bind)
        
        print(f"✓ Successfully collected {len(driver_standings_df)} driver standings entries")
        print(f"✓ Successfully collected {len(constructor_standings_df)} constructor standings entries")
        return True
    except Exception as e:
        print(f"✗ Standings collection failed: {str(e)}")
        return False

def run_all_tests():
    print("Starting F1 data collection tests...")
    
    if not test_database_setup():
        return
        
    if not test_race_collection():
        return
        
    if not test_standings_collection():
        return
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    run_all_tests()