import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from database.database import Database
from database.models import Race, Result, DriverStanding, ConstructorStanding
from database.models import Weather
from sqlalchemy.orm import Session
from sqlalchemy import text

def collect_race_data(start_year: int = 1950, end_year: int = 2020):
    """
    Collects race data from Ergast API, following the same approach as the example
    but storing in SQLite instead of CSV files.
    
    This function mirrors the original example's simplicity while adding database storage.
    """
    db = Database()
    session = db.get_session()

    try:
        # We'll collect races year by year, just like the example
        for year in range(start_year, end_year + 1):
            print(f"Collecting data for {year}...")
            
            # Get basic race information
            url = f'https://ergast.com/api/f1/{year}.json'
            r = requests.get(url)
            json = r.json()

            for item in json['MRData']['RaceTable']['Races']:
                # Create race entry in database
                race = Race(
                    season=int(item['season']),
                    round=int(item['round']),
                    circuit_id=item['Circuit']['circuitId'],
                    country=item['Circuit']['Location']['country'],
                    date=item['date']
                )
                session.add(race)
                session.flush()  # This gets us the race ID

                # Now get results for this race
                results_url = f'http://ergast.com/api/f1/{year}/{item["round"]}/results.json'
                results_r = requests.get(results_url)
                results_json = results_r.json()

                # Add results to database
                for result in results_json['MRData']['RaceTable']['Races'][0]['Results']:
                    result_entry = Result(
                        race_id=race.id,
                        driver=result['Driver']['driverId'],
                        date_of_birth=result['Driver']['dateOfBirth'],
                        nationality=result['Driver']['nationality'],
                        constructor=result['Constructor']['constructorId'],
                        grid=int(result['grid']),
                        position=int(result['position']) if result['position'].isdigit() else None,
                        points=float(result['points']),
                        status=result['status']
                    )
                    session.add(result_entry)

                # Commit after each race to save our progress
                session.commit()

    except Exception as e:
        print(f"Error collecting data: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


