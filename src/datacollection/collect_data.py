import requests
import pandas as pd
from typing import List
from datetime import datetime
from bs4 import BeautifulSoup
from database.database import Database
from database.models import Qualifying, Race, Result, DriverStanding, ConstructorStanding

def collect_race_data(start_year: int = 1950, end_year: int = 2020):

    db = Database()
    session = db.get_session()

    try:
        for year in range(start_year, end_year + 1):
            print(f"Collecting data for {year}...")
            
            url = f'https://ergast.com/api/f1/{year}.json'
            r = requests.get(url)
            json = r.json()

            for item in json['MRData']['RaceTable']['Races']:
                race = Race(
                    season=int(item['season']),
                    round=int(item['round']),
                    circuit_id=item['Circuit']['circuitId'],
                    country=item['Circuit']['Location']['country'],
                    date=datetime.strptime(item['date'], '%Y-%m-%d').date()
                )
                session.add(race)
                session.flush()

                results_url = f'http://ergast.com/api/f1/{year}/{item["round"]}/results.json'
                results_r = requests.get(results_url)
                results_json = results_r.json()

                for result in results_json['MRData']['RaceTable']['Races'][0]['Results']:
                    result_entry = Result(
                        race_id=race.id,
                        driver=result['Driver']['driverId'],
                        date_of_birth=datetime.strptime(result['Driver']['dateOfBirth'], '%Y-%m-%d').date(),
                        nationality=result['Driver']['nationality'],
                        constructor=result['Constructor']['constructorId'],
                        grid=int(result['grid']),
                        position=int(result['position']) if result['position'].isdigit() else None,
                        points=float(result['points']),
                        status=result['status']
                    )
                    session.add(result_entry)

                session.commit()

    except Exception as e:
        print(f"Error collecting data: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def collect_driver_standings(rounds: List[List]):

    db = Database()
    session = db.get_session()

    try:
        for year_rounds in rounds:
            year = year_rounds[0]
            round_numbers = year_rounds[1]
            
            print(f"Collecting driver standings for {year}...")
            
            for round_num in round_numbers:
                url = f'https://ergast.com/api/f1/{year}/{round_num}/driverStandings.json'
                r = requests.get(url)
                json = r.json()

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
                
                session.commit()

    except Exception as e:
        print(f"Error collecting driver standings: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def collect_constructor_standings(rounds: List[List]):

    db = Database()
    session = db.get_session()

    try:
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

    rounds = []
    for year in races_data.season.unique():
        rounds.append([year, list(races_data[races_data.season == year]['round'])])
    return rounds

def collect_qualifying_data(start_year: int = 1983, end_year: int = 2020):

    db = Database()
    session = db.get_session()

    try:
        for year in range(start_year, end_year + 1):
            print(f"Collecting qualifying data for {year}...")
            
            url = f'https://ergast.com/api/f1/{year}/qualifying.json'
            r = requests.get(url)
            json = r.json()

            for item in json['MRData']['RaceTable']['Races']:
                for quali in item['QualifyingResults']:
                    quali_entry = Qualifying(
                        season=int(item['season']),
                        round=int(item['round']),
                        driver=quali['Driver']['driverId'],
                        grid_position=int(quali['position']),
                        qualifying_time=quali['Q3'] if 'Q3' in quali else (
                            quali['Q2'] if 'Q2' in quali else quali['Q1']),
                        car=quali['Constructor']['constructorId']
                    )
                    session.add(quali_entry)
                
            session.commit()

    except Exception as e:
        print(f"Error collecting qualifying data: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()
    
def collect_qualifying_data(start_year: int = 1983, end_year: int = 2020):
    """
    Collects qualifying data from Formula 1 races. We start from 1983 because
    that's when consistent qualifying data becomes available in the format we need.
    
    This function follows the example's approach of gathering qualifying times
    and positions, but stores them in our SQLite database instead of CSV files.
    """
    db = Database()
    session = db.get_session()

    try:
        qualifying_results = []
        for year in range(start_year, end_year + 1):
            print(f"Collecting qualifying data for {year}...")
            
            # Get the races for this year
            url = f'https://www.formula1.com/en/results.html/{year}/races.html'
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Find all race links for this year
            year_links = []
            for page in soup.find_all('a', attrs={'class': "resultsarchive-filter-item-link FilterTrigger"}):
                link = page.get('href')
                if f'/en/results.html/{year}/races/' in link:
                    year_links.append(link)

            # Process each race's qualifying results
            for n, link in enumerate(year_links, 1):
                try:
                    # Modify link to point to qualifying results instead of race results
                    quali_link = link.replace('race-result.html', 'starting-grid.html')
                    quali_url = f'https://www.formula1.com{quali_link}'
                    
                    # Read qualifying data table
                    df = pd.read_html(quali_url)[0]
                    df['season'] = year
                    df['round'] = n
                    
                    # Clean up the qualifying times and positions
                    df = df.rename(columns={'Pos': 'grid_position', 'Driver': 'driver_name', 'Car': 'car',
                                          'Time': 'qualifying_time'})
                    
                    # Store in database
                    for _, row in df.iterrows():
                        qualifying_entry = Qualifying(
                            season=year,
                            round=n,
                            grid_position=row['grid_position'],
                            driver_name=row['driver_name'],
                            car=row['car'],
                            qualifying_time=row['qualifying_time'] if pd.notna(row['qualifying_time']) else None
                        )
                        session.add(qualifying_entry)
                    
                    session.commit()
                    print(f"Stored qualifying data for {year} Round {n}")
                    
                except Exception as e:
                    print(f"Error collecting qualifying data for {year} Round {n}: {str(e)}")
                    continue

    except Exception as e:
        print(f"Error in qualifying data collection: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    
    db = Database()
    db.create_tables()
    
    collect_race_data(start_year=2015, end_year=2020)  
    
    session = db.get_session()
    races_df = pd.read_sql("SELECT * FROM races", session.bind)
    rounds = get_rounds(races_df)
    
    collect_driver_standings(rounds)
    collect_constructor_standings(rounds)