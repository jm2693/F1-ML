from database.database import Database
from data_collection.races import F1DataCollector

def main():
    # Initialize database
    db = Database()
    db.create_tables()
    

if __name__ == "__main__":
    main()