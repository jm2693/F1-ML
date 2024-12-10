from database.database import Database
from src.datacollection.collect_data import (
    collect_qualifying_data,
    collect_race_data, 
    collect_driver_standings,
    collect_constructor_standings, 
    get_rounds
)
from src.datacollection.cleandata import clean_and_aggregate_data
from src.model.train_model import save_model, train_model, load_cleaned_data, prepare_data
from src.model.test_model import evaluate_model
from src.model.predict import F1Predictor

import pandas as pd
import os

def test_data_collection(start_year=2015, end_year=2020):

    print("\n1. Testing Data Collection...")
    try:
        db = Database()
        db.create_tables()
        
        print("Collecting race data...")
        collect_race_data(start_year=start_year, end_year=end_year)
        
        session = db.get_session()
        races_df = pd.read_sql("SELECT * FROM races", session.bind)
        rounds = get_rounds(races_df)
        
        print("Collecting standings data...")
        collect_driver_standings(rounds)
        collect_constructor_standings(rounds)
        
        print("Collecting qualifying data...")
        collect_qualifying_data(start_year=start_year, end_year=end_year)
        
        results = pd.read_sql("SELECT COUNT(*) as count FROM races", session.bind).iloc[0]['count']
        print(f"Collected {results} races")
        
        return True
        
    except Exception as e:
        print(f"Data collection failed: {str(e)}")
        return False

def test_data_cleaning():

    print("\n2. Testing Data Cleaning...")
    try:
        cleaning_success = clean_and_aggregate_data("f1_prediction.db")
        
        if cleaning_success:
            db = Database()
            session = db.get_session()
            
            driver_agg = pd.read_sql("SELECT COUNT(*) as count FROM driver_aggregates", 
                                   session.bind).iloc[0]['count']
            constructor_agg = pd.read_sql("SELECT COUNT(*) as count FROM constructor_aggregates", 
                                        session.bind).iloc[0]['count']
            
            print(f"Created {driver_agg} driver aggregates")
            print(f"Created {constructor_agg} constructor aggregates")
            return True
        else:
            print("Data cleaning process failed")
            return False
            
    except Exception as e:
        print(f"Data cleaning failed: {str(e)}")
        return False

def test_model_training():
    """
    Tests the model training process.
    """
    print("\n3. Testing Model Training...")
    try:
        
        database_path = "f1_prediction.db"
        model_output_path = "models/f1_winner_predictor.pkl"
    
        os.makedirs('models', exist_ok=True)
        
        print("Loading cleaned data...")
        driver_aggregates = load_cleaned_data(database_path)
        
        print("Preparing data for training...")
        X_train, X_test, y_train, y_test, scaler = prepare_data(driver_aggregates)
        
        print("Training model...")
        model = train_model(X_train, y_train)
        
        print("Evaluating model...")
        evaluate_model(model, X_test, y_test)
        
        print("Saving model and scaler...")
        save_model(model, scaler, model_output_path)
        
        print(f"Model saved to {model_output_path}")
        
        return True
        
    except Exception as e:
        print(f"Model training failed: {str(e)}")
        return False

def test_predictions():

    print("\n4. Testing Predictions...")
    try:
        predictor = F1Predictor(
            model_path='models/f1_winner_predictor.pkl',
            database_path='f1_prediction.db'
        )
        
        print("Generating predictions...")
        predictions = predictor.predict_champions()
        
        print("\nSample Predictions:")
        print(predictions.head())
        
        return True
        
    except Exception as e:
        print(f"Predictions failed: {str(e)}")
        return False

def run_full_system():
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
    run_full_system()