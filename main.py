from src.datacollection.collect_data import collect_race_data, collect_driver_standings, get_rounds
from src.datacollection.collect_data import collect_constructor_standings, collect_weather_data
from database.database import Database
import pandas as pd

def main():
    # Setup database
    db = Database()
    db.create_tables()
    
    # Collect base race data
    print("Collecting race data...")
    collect_race_data(start_year=2015, end_year=2020)  # Using smaller range for testing
    
    # Get race data for rounds information
    session = db.get_session()
    races_df = pd.read_sql("SELECT * FROM races", session.bind)
    rounds = get_rounds(races_df)
    
    # Collect standings data
    print("Collecting standings data...")
    collect_driver_standings(rounds)
    collect_constructor_standings(rounds)
    
    # Collect weather data
    print("Collecting weather data...")
    collect_weather_data(races_df)
    
    print("Data collection complete!")

if __name__ == "__main__":
    main()