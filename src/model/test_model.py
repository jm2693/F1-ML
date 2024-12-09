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
    X = driver_aggregates.drop(columns=["is_champion", "season", "driver"])
    y = driver_aggregates["is_champion"]
    return X, y

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
    y_pred = model.predict(X)
    accuracy = accuracy_score(y, y_pred)
    print("Model Accuracy:", accuracy)
    print("Classification Report:\n", classification_report(y, y_pred))
    return accuracy

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