import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib  

def load_cleaned_data(database_path):

    engine = create_engine(f'sqlite:///{database_path}')
    driver_aggregates = pd.read_sql("SELECT * FROM driver_aggregates", engine)
    return driver_aggregates

def prepare_data(driver_aggregates):

    from sklearn.preprocessing import StandardScaler
    
    numeric_columns = driver_aggregates.select_dtypes(include=['float64', 'int64']).columns
    driver_aggregates[numeric_columns] = driver_aggregates[numeric_columns].fillna(0)
    
    train_seasons = driver_aggregates['season'] < 2019  # Use pre-2019 for training
    
    feature_columns = [
        'total_points', 'total_wins', 'podiums', 'avg_grid_position',
        'avg_final_position', 'total_races', 'dnf_races', 'mechanical_failures',
        'finish_rate', 'consistency_score', 'qualifying_performance',
        'comeback_drives', 'points_per_race'
    ]
    
    scaler = StandardScaler()
    X = driver_aggregates[feature_columns]
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=feature_columns)
    
    X_train = X_scaled[train_seasons]
    X_test = X_scaled[~train_seasons]
    y_train = driver_aggregates[train_seasons]['is_champion']
    y_test = driver_aggregates[~train_seasons]['is_champion']
    
    return X_train, X_test, y_train, y_test, scaler 

def train_model(X_train, y_train):

    from sklearn.ensemble import RandomForestClassifier
    
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        class_weight='balanced',  # Handle class imbalance
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # Print feature importances
    feature_importance = pd.DataFrame({
        'feature': X_train.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nFeature Importances:")
    print(feature_importance)
    
    return model

def evaluate_model(model, X_test, y_test):

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print("Model Accuracy:", accuracy)
    print("Classification Report:\n", classification_report(y_test, y_pred))
    
    return accuracy

def save_model(model, scaler, output_path):
    joblib.dump(model, output_path)
    joblib.dump(scaler, output_path.replace('.pkl', '_scaler.pkl'))
    print(f"Model and scaler saved to {output_path}")

if __name__ == "__main__":
    database_path = "f1_prediction.db"
    model_output_path = "models/f1_winner_predictor.pkl"
    
    print("Loading cleaned data...")
    driver_aggregates = load_cleaned_data(database_path)
    X_train, X_test, y_train, y_test, scaler = prepare_data(driver_aggregates)
    
    print("Training model...")
    model = train_model(X_train, y_train)
    
    print("Evaluating model...")
    evaluate_model(model, X_test, y_test)
    
    save_model(model, scaler, model_output_path)
    
