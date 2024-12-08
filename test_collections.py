import pandas as pd
from database.database import Database
from src.datacollection.collect_data import (
    collect_race_data, 
    collect_driver_standings,
    collect_constructor_standings, 
    collect_weather_data,
    get_rounds
)

def test_database_setup():
    """
    Tests that our database can be created and connected to properly.
    This is our foundation - if this doesn't work, nothing else will.
    """
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
    """
    Tests the collection of race data for a small sample period.
    We use just one year of data to keep the test quick but meaningful.
    """
    print("\nTesting race data collection...")
    try:
        collect_race_data(start_year=2020, end_year=2020)
        
        # Verify the data was stored
        db = Database()
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races WHERE season = 2020", session.bind)
        results_df = pd.read_sql("SELECT * FROM results WHERE race_id IN (SELECT id FROM races WHERE season = 2020)", session.bind)
        
        print(f"✓ Successfully collected {len(races_df)} races and {len(results_df)} results from 2020")
        print(f"Sample race data:\n{races_df.head()}")
        return True
    except Exception as e:
        print(f"✗ Race collection failed: {str(e)}")
        return False

def test_standings_collection():
    """
    Tests the collection of standings data.
    We'll use the races we just collected to get the standings data.
    """
    print("\nTesting standings collection...")
    try:
        db = Database()
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races", session.bind)
        rounds = get_rounds(races_df)
        
        collect_driver_standings(rounds)
        collect_constructor_standings(rounds)
        
        # Verify the data
        driver_standings_df = pd.read_sql("SELECT * FROM driver_standings WHERE season = 2020", session.bind)
        constructor_standings_df = pd.read_sql("SELECT * FROM constructor_standings WHERE season = 2020", session.bind)
        
        print(f"✓ Successfully collected {len(driver_standings_df)} driver standings entries")
        print(f"✓ Successfully collected {len(constructor_standings_df)} constructor standings entries")
        print(f"Sample driver standings:\n{driver_standings_df.head()}")
        return True
    except Exception as e:
        print(f"✗ Standings collection failed: {str(e)}")
        return False

def test_weather_collection():
    """
    Tests the weather data collection.
    This might take longer as it involves web scraping.
    """
    print("\nTesting weather collection...")
    try:
        db = Database()
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races", session.bind)
        
        collect_weather_data(races_df)
        
        # Verify the data
        weather_df = pd.read_sql("SELECT * FROM weather WHERE season = 2020", session.bind)
        
        print(f"✓ Successfully collected weather data for {len(weather_df)} races")
        print(f"Sample weather data:\n{weather_df.head()}")
        return True
    except Exception as e:
        print(f"✗ Weather collection failed: {str(e)}")
        return False

def run_all_tests():
    """
    Runs all tests in sequence, stopping if any test fails.
    """
    print("Starting F1 data collection tests...")
    
    if not test_database_setup():
        return
        
    if not test_race_collection():
        return
        
    if not test_standings_collection():
        return
        
    test_weather_collection()
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    run_all_tests()