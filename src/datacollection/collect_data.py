import pandas as pd
# import numpy as np
import requests
from datetime import datetime
# from bs4 import BeautifulSoup
from database.database import Database
from database.models import Qualifying, Race, Result, DriverStanding, ConstructorStanding, Weather
# from sqlalchemy.orm import Session
from typing import List

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
                # Create race entry
                race = Race(
                    season=int(item['season']),
                    round=int(item['round']),
                    circuit_id=item['Circuit']['circuitId'],
                    country=item['Circuit']['Location']['country'],
                    date=datetime.strptime(item['date'], '%Y-%m-%d').date()
                )
                session.add(race)
                session.flush()

                # Get results for this race
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
    """
    Args:
        races_data: The races table from our database
        
    Returns:
        List of [year, [round_numbers]] pairs
    """
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

def collect_weather_data(races_data):

    db = Database()
    session = db.get_session()

    weather_dict = {
        'weather_warm': ['soleggiato', 'clear', 'warm', 'hot', 'sunny', 'fine', 'mild', 'sereno'],
        'weather_cold': ['cold', 'fresh', 'chilly', 'cool'],
        'weather_dry': ['dry', 'asciutto'],
        'weather_wet': ['showers', 'wet', 'rain', 'pioggia', 'damp', 'thunderstorms', 'rainy'],
        'weather_cloudy': ['overcast', 'nuvoloso', 'clouds', 'cloudy', 'grey', 'coperto']
    }

    try:
        for _, race in races_data.iterrows():
            print(f"Collecting weather data for {race['season']} Round {race['round']}...")
            
            try:
                weather_text = scrape_weather_from_wiki(race['url'])
            except:
                weather_text = 'not found'

            weather_conditions = {
                category: 1 if any(word in weather_text.lower() for word in terms) else 0
                for category, terms in weather_dict.items()
            }

            weather_entry = Weather(
                season=race['season'],
                round=race['round'],
                circuit_id=race['circuit_id'],
                weather_description=weather_text,
                **weather_conditions
            )
            session.add(weather_entry)
            
            if _ % 50 == 0: 
                session.commit()

        session.commit()

    except Exception as e:
        print(f"Error collecting weather data: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()
        
def scrape_weather_from_wiki(url):

    try:
        df = pd.read_html(url)[0]
        if 'Weather' in list(df.iloc[:,0]):
            n = list(df.iloc[:,0]).index('Weather')
            return df.iloc[n,1]
        else:
            for i in range(1, 4):
                try:
                    df = pd.read_html(url)[i]
                    if 'Weather' in list(df.iloc[:,0]):
                        n = list(df.iloc[:,0]).index('Weather')
                        return df.iloc[n,1]
                except:
                    continue
                    
            driver = webdriver.Chrome()
            driver.get(url)
            
            button = driver.find_element_by_link_text('Italiano')
            button.click()
            
            clima = driver.find_element_by_xpath('//*[@id="mw-content-text"]/div/table[1]/tbody/tr[9]/td').text
            return clima
    except:
        return 'not found'

if __name__ == "__main__":
    
    db = Database()
    db.create_tables()
    
    collect_race_data(start_year=2015, end_year=2020)  
    
    session = db.get_session()
    races_df = pd.read_sql("SELECT * FROM races", session.bind)
    rounds = get_rounds(races_df)
    
    collect_driver_standings(rounds)
    collect_constructor_standings(rounds)
    collect_weather_data(races_df)