# F1 Season Winner Predictor

## Overview

We are using historical F1 data from 1950 - 2020 in order to create a prediction for the 2021 season. 
We will then compare this to the results of the actual 2021 season.

Difficulties:
- There have been many changes over the decades in the F1 sport. 
- Qualifying times and rounds need to be normalized and properly collected.
- There is also a lot of missing data from earlier decades and extra data from recent years.

### High Level Overview

1. Use the Ergast F1 API to collect data (this is what most Kaggle datasets use)
2. Store this data in a sqlite database (we also use sqlalchemy to better interact with our sqlite engine)
3. Clean, merge, and feature engineer our collected data from the database, and store in a cleaned table
4. Split the cleaned data 80/20. Use the training set to traing our Forest Regressor Model. Test with testing set
5. Use our model to predict the 2021 season results


## GETTING STARTED:

### We're using a requirements.txt file to specify all the external libraries and their versions that our project needs to run.

1. start a virtual environment with

    - python3 -m venv venv
    - source venv/bin/activate
    - pip install -r requirements.txt

2. test database connection and data collection

    - python3 test_collections.py