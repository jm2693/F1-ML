from sqlalchemy import Column, Integer, String, Float, Boolean, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Race(Base):
    __tablename__ = 'races'
    
    id = Column(Integer, primary_key=True)
    season = Column(Integer)
    round = Column(Integer)
    circuit_id = Column(String)
    country = Column(String)
    date = Column(Date)

class Result(Base):
    __tablename__ = 'results'
    
    id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('races.id'))
    driver = Column(String)
    date_of_birth = Column(Date)
    nationality = Column(String)
    constructor = Column(String)
    grid = Column(Integer)
    position = Column(Integer)
    points = Column(Float)
    status = Column(String)

class DriverStanding(Base):
    __tablename__ = 'driver_standings'
    
    id = Column(Integer, primary_key=True)
    season = Column(Integer)
    round = Column(Integer)
    driver = Column(String)
    points = Column(Float)
    wins = Column(Integer)
    position = Column(Integer)

class ConstructorStanding(Base):
    __tablename__ = 'constructor_standings'
    
    id = Column(Integer, primary_key=True)
    season = Column(Integer)
    round = Column(Integer)
    constructor = Column(String)
    points = Column(Float)
    wins = Column(Integer)
    position = Column(Integer)

class Weather(Base):
    __tablename__ = 'weather'
    
    id = Column(Integer, primary_key=True)
    season = Column(Integer)
    round = Column(Integer)
    circuit_id = Column(String)
    weather_description = Column(String)
    weather_warm = Column(Boolean)
    weather_cold = Column(Boolean)
    weather_dry = Column(Boolean)
    weather_wet = Column(Boolean)
    weather_cloudy = Column(Boolean)
    
class Qualifying(Base):
    __tablename__ = 'qualifying'
    
    id = Column(Integer, primary_key=True)
    season = Column(Integer)
    round = Column(Integer)
    driver = Column(String)
    grid_position = Column(Integer)
    qualifying_time = Column(String)
    car = Column(String)