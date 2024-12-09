from database.database import Database
from src.datacollection.collect_data import (
    collect_race_data, 
    collect_driver_standings,
    collect_constructor_standings, 
    collect_weather_data,
    get_rounds
)
from src.datacollection.cleandata import clean_and_aggregate_data
from src.model.train_model import train_model, load_cleaned_data, prepare_data, evaluate_model
from src.model.predict import F1Predictor

import pandas as pd
import os
import sys

def test_data_collection(start_year=2015, end_year=2020):
    """
    Tests the data collection process.
    Uses a smaller date range for faster testing.
    """
    print("\n1. Testing Data Collection...")
    try:
        # Setup database
        db = Database()
        db.create_tables()
        
        # Collect race data
        print("  Collecting race data...")
        collect_race_data(start_year=start_year, end_year=end_year)
        
        # Get rounds information
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races", session.bind)
        rounds = get_rounds(races_df)
        
        # Collect standings
        print("  Collecting standings data...")
        collect_driver_standings(rounds)
        collect_constructor_standings(rounds)
        
        # Collect weather data
        print("  Collecting weather data...")
        collect_weather_data(races_df)
        
        # Verify data collection
        results = pd.read_sql("SELECT COUNT(*) as count FROM races", session.bind).iloc[0]['count']
        print(f"  ✓ Collected {results} races")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Data collection failed: {str(e)}")
        return False

def test_data_cleaning():
    """
    Tests the data cleaning and aggregation process.
    """
    print("\n2. Testing Data Cleaning...")
    try:
        clean_and_aggregate_data("f1_prediction.db")
        
        # Verify cleaned data exists
        db = Database()
        session = db.get_session()
        
        # Check if aggregated tables exist
        driver_agg = pd.read_sql("SELECT COUNT(*) as count FROM driver_aggregates", session.bind).iloc[0]['count']
        constructor_agg = pd.read_sql("SELECT COUNT(*) as count FROM constructor_aggregates", session.bind).iloc[0]['count']
        
        print(f"  ✓ Created {driver_agg} driver aggregates")
        print(f"  ✓ Created {constructor_agg} constructor aggregates")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Data cleaning failed: {str(e)}")
        return False

def test_model_training():
    """
    Tests the model training process.
    """
    print("\n3. Testing Model Training...")
    try:
        # Make sure models directory exists
        os.makedirs('models', exist_ok=True)
        
        # Load data and train model
        driver_aggregates = load_cleaned_data("f1_prediction.db")
        X_train, X_test, y_train, y_test = prepare_data(driver_aggregates)
        
        print("  Training model...")
        model = train_model(X_train, y_train)
        
        print("  Evaluating model...")
        accuracy = evaluate_model(model, X_test, y_test)
        
        # Save model
        model_path = "models/f1_winner_predictor.pkl"
        model.save(model_path)
        print(f"  ✓ Model saved to {model_path}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Model training failed: {str(e)}")
        return False

def test_predictions():
    """
    Tests the prediction functionality.
    """
    print("\n4. Testing Predictions...")
    try:
        predictor = F1Predictor(
            model_path='models/f1_winner_predictor.pkl',
            database_path='f1_prediction.db'
        )
        
        print("  Generating predictions...")
        predictions = predictor.predict_champions()
        
        print("\n  Sample Predictions:")
        print(predictions.head())
        
        return True
        
    except Exception as e:
        print(f"  ✗ Predictions failed: {str(e)}")
        return False

def run_full_system_test():
    """
    Runs all system tests in sequence.
    """
    print("Starting F1 Prediction System Test")
    print("=" * 50)
    
    if not test_data_collection():
        print("\nTest failed at data collection stage")
        return
        
    if not test_data_cleaning():
        print("\nTest failed at data cleaning stage")
        return
        
    if not test_model_training():
        print("\nTest failed at model training stage")
        return
        
    if not test_predictions():
        print("\nTest failed at prediction stage")
        return
        
    print("\n✓ All system tests completed successfully!")

if __name__ == "__main__":
    run_full_system_test()