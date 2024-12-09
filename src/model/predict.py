import pandas as pd
import joblib
from sqlalchemy import create_engine
from datetime import datetime

class F1Predictor:

    def __init__(self, model_path, database_path):
        self.model = joblib.load(model_path)
        self.scaler = joblib.load(model_path.replace('.pkl', '_scaler.pkl'))
        self.database_path = database_path
        self.engine = create_engine(f'sqlite:///{database_path}')

    def get_latest_driver_data(self):

        query = """
        SELECT * FROM driver_aggregates 
        WHERE season = (SELECT MAX(season) FROM driver_aggregates)
        """
        return pd.read_sql(query, self.engine)

    def predict_champions(self, driver_data=None):

        if driver_data is None:
            driver_data = self.get_latest_driver_data()

        feature_columns = [
            'total_points', 'total_wins', 'podiums', 'avg_grid_position',
            'avg_final_position', 'total_races', 'dnf_races', 
            'mechanical_failures', 'finish_rate', 'consistency_score', 
            'qualifying_performance', 'comeback_drives', 'points_per_race'
        ]
        
        X_new = driver_data[feature_columns]
        X_new_scaled = self.scaler.transform(X_new)
        
        predictions = self.model.predict(X_new_scaled)
        probabilities = self.model.predict_proba(X_new_scaled)
        
        results = pd.DataFrame({
            'driver': driver_data['driver'],
            'season': driver_data['season'],
            'predicted_champion': predictions,
            'championship_probability': probabilities[:, 1],
            'current_points': driver_data['total_points'],
            'wins_this_season': driver_data['total_wins']
        })
        
        return results.sort_values('championship_probability', ascending=False)

    def print_championship_forecast(self):

        predictions = self.predict_champions()
        
        print("\nF1 Championship Prediction Report")
        print("=" * 50)
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Print top 5 championship contenders
        print("Top Championship Contenders:")
        top_5 = predictions.head()
        for _, driver in top_5.iterrows():
            print(f"\n{driver['driver']}:")
            print(f"  Championship Probability: {driver['championship_probability']*100:.1f}%")
            print(f"  Current Points: {driver['current_points']}")
            print(f"  Wins This Season: {driver['wins_this_season']}")

def main():
    """
    Main function to demonstrate prediction functionality.
    """
    predictor = F1Predictor(
        model_path='models/f1_winner_predictor.pkl',
        database_path='f1_prediction.db'
    )
    
    print("\nGenerating championship predictions...")
    predictor.print_championship_forecast()

if __name__ == "__main__":
    main()