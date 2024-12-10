import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import accuracy_score, classification_report
import joblib  

def load_cleaned_data(database_path):
    """
    Load the cleaned and aggregated data from the SQLite database.
    """
    
    engine = create_engine(f'sqlite:///{database_path}')
    driver_aggregates = pd.read_sql("SELECT * FROM driver_aggregates", engine)
    return driver_aggregates

def prepare_test_data(driver_aggregates):
    """
    Prepare test data for evaluation.
    """
    # Features and target
    
    feature_columns = [
        'total_points', 'total_wins', 'podiums', 'avg_grid_position',
        'avg_final_position', 'total_races', 'dnf_races', 'mechanical_failures',
        'finish_rate', 'consistency_score', 'qualifying_performance',
        'comeback_drives', 'points_per_race'
    ]
    
    scaler = joblib.load('models/f1_winner_predictor_scaler.pkl')
    
    # Select features in the same order
    X = driver_aggregates[feature_columns]
    
    # Scale features using the same scaler used in training
    X_scaled = scaler.transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=feature_columns)
    
    # Get target variable
    y = driver_aggregates["is_champion"]
    
    return X_scaled, y

def load_model(model_path):
    """
    Load the trained model from the saved file.
    """
    model = joblib.load(model_path)
    print(f"Model loaded from {model_path}")
    return model

def evaluate_model(model, X, y):
    """
    Evaluate the model on the test data.
    """
    
    try:
        # Verify feature names match what the model expects
        expected_features = model.feature_names_in_
        actual_features = X.columns.tolist()
        
        if set(expected_features) != set(actual_features):
            print("Feature mismatch!")
            print("Expected features:", expected_features)
            print("Got features:", actual_features)
            return None
            
        # Make predictions
        y_pred = model.predict(X)
        
        # Calculate metrics
        accuracy = accuracy_score(y, y_pred)
        print("\nModel Accuracy:", accuracy)
        print("\nClassification Report:")
        print(classification_report(y, y_pred))
        
        return accuracy
        
    except Exception as e:
        print(f"Error during evaluation: {str(e)}")
        raise

if __name__ == "__main__":
    # Paths to database and model
    database_path = "f1_prediction.db"  # Path to the SQLite database
    model_path = "models/f1_winner_predictor.pkl"  # Path to the saved model

    # Load data and model
    print("Loading test data...")
    driver_aggregates = load_cleaned_data(database_path)
    X_test, y_test = prepare_test_data(driver_aggregates)

    print("Loading trained model...")
    model = load_model(model_path)

    # Evaluate the model
    print("Evaluating model on test data...")
    evaluate_model(model, X_test, y_test)