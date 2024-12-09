import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib  # To save the trained model

def load_cleaned_data(database_path):
    """
    Load the cleaned and aggregated data from the SQLite database.
    """
    engine = create_engine(f'sqlite:///{database_path}')
    driver_aggregates = pd.read_sql("SELECT * FROM driver_aggregates", engine)
    return driver_aggregates

def prepare_data(driver_aggregates):
    """
    Prepare the cleaned data for training the model.
    - Splits data into features (X) and target (y).
    - Ensures only relevant columns are used.
    """
    # Features and target
    X = driver_aggregates.drop(columns=["is_champion", "season", "driver"])
    y = driver_aggregates["is_champion"]
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

def train_model(X_train, y_train):
    """
    Train a Random Forest model on the training data.
    """
    model = RandomForestClassifier(random_state=42, n_estimators=100)
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    """
    Evaluate the trained model on the test data.
    """
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print("Model Accuracy:", accuracy)
    print("Classification Report:\n", classification_report(y_test, y_pred))
    return accuracy

def save_model(model, output_path):
    """
    Save the trained model to a file for later use.
    """
    joblib.dump(model, output_path)
    print(f"Model saved to {output_path}")

if __name__ == "__main__":
    # Database and model paths
    database_path = "f1_prediction.db"  # Path to the SQLite database
    model_output_path = "models/f1_winner_predictor.pkl"  # Path to save the trained model

    # Load and prepare data
    print("Loading cleaned data...")
    driver_aggregates = load_cleaned_data(database_path)
    X_train, X_test, y_train, y_test = prepare_data(driver_aggregates)

    # Train the model
    print("Training model...")
    model = train_model(X_train, y_train)

    # Evaluate the model
    print("Evaluating model...")
    evaluate_model(model, X_test, y_test)

    # Save the trained model
    save_model(model, model_output_path)