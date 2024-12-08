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




def collect_driver_standings(rounds: List[List]):
    """
    Collects driver standings data from the Ergast API. This function mirrors the example's
    approach where we iterate through each round of each season to gather standings data.
    
    The 'rounds' parameter is a list of [year, [round_numbers]] pairs, just like in the example.
    """
    db = Database()
    session = db.get_session()

    try:
        # Iterate through each season and its rounds
        for year_rounds in rounds:
            year = year_rounds[0]
            round_numbers = year_rounds[1]
            
            print(f"Collecting driver standings for {year}...")
            
            # For each round in the season
            for round_num in round_numbers:
                url = f'https://ergast.com/api/f1/{year}/{round_num}/driverStandings.json'
                r = requests.get(url)
                json = r.json()

                # Get the standings list
                standings_list = json['MRData']['StandingsTable']['StandingsLists'][0]
                
                for standing in standings_list['DriverStandings']:
                    standing_entry = DriverStanding(
                        season=year,
                        round=round_num,
                        driver=standing['Driver']['driverId'],
                        points=float(standing['points']),
                        wins=int(standing['wins']),
                        position=int(standing['position'])
                    )
                    session.add(standing_entry)
                
                # Commit after each round to save progress
                session.commit()

    except Exception as e:
        print(f"Error collecting driver standings: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def collect_constructor_standings(rounds: List[List]):
    """
    Collects constructor standings data. The constructor championship started in 1958,
    so we'll need to handle this special case just like the example did.
    """
    db = Database()
    session = db.get_session()

    try:
        # Constructor championship started in 1958
        constructor_rounds = rounds[8:]  # Skip first 8 years
        
        for year_rounds in constructor_rounds:
            year = year_rounds[0]
            round_numbers = year_rounds[1]
            
            print(f"Collecting constructor standings for {year}...")
            
            for round_num in round_numbers:
                url = f'https://ergast.com/api/f1/{year}/{round_num}/constructorStandings.json'
                r = requests.get(url)
                json = r.json()

                standings_list = json['MRData']['StandingsTable']['StandingsLists'][0]
                
                for standing in standings_list['ConstructorStandings']:
                    standing_entry = ConstructorStanding(
                        season=year,
                        round=round_num,
                        constructor=standing['Constructor']['constructorId'],
                        points=float(standing['points']),
                        wins=int(standing['wins']),
                        position=int(standing['position'])
                    )
                    session.add(standing_entry)
                
                session.commit()

    except Exception as e:
        print(f"Error collecting constructor standings: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def get_rounds(races_data):
    """
    Helper function to create the rounds list structure used in the example.
    This helps us maintain the same data organization pattern.
    
    Args:
        races_data: The races table from our database
        
    Returns:
        List of [year, [round_numbers]] pairs
    """
    rounds = []
    for year in races_data.season.unique():
        rounds.append([year, list(races_data[races_data.season == year]['round'])])
    return rounds

