#################################################
# Import Dependencies
#################################################
import numpy as np
import pandas as pd
import datetime as dt 

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measure = Base.classes.measurement
station = Base.classes.station

#################################################
# Flask Setup
#################################################
# --- create an instance of the Flask class ---
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available api routes."""
    print("Server requested climate app home page...")
    return (
        f"Welcome to the Climate App of Hawaii!<br/>"
        f"----------------------------------<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start date<br/>"
        f"/api/v1.0/start date/end date<br/>"
        f"<br>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    '''
    This gives the precipitation in json format for date and precipitation in the last year
    '''
    # --- create a session from Python to the database ---
    session = Session(engine)

    # --- perform a query to retrieve all the date and precipitation values ---
    max_dt = session.query(func.max(measure.date)).all()
    recent_dt= dt.datetime.strptime(max_dt[0][0],"%Y-%m-%d").date()
    one_year_dt = recent_dt - dt.timedelta(days=365)
    qry = session.query(measure.date,measure.prcp).filter(measure.date <= recent_dt).\
        filter(measure.date >= one_year_dt)

    # --- close the session ---
    session.close()

    # --- convert the query results to a dictionary using date as the key and prcp as the value ---
    prcp_dict = {} 
    for date, prcp in qry:
        # if not np.isnan(prcp):
        prcp_dict[date] = prcp
    
    # Return the JSON representation of your dictionary.
    return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
    '''
    This will give a list of stations available to review
    '''
    # --- create a session from Python to the database ---
    session = Session(engine)
    
    # --- perform a query to retrieve all the station data ---
    results = session.query(station.id, station.station, station.name).all()

    # --- close the session ---
    session.close()

    # --- create a list of dictionaries with station info using for loop---
    list_stations = []

    for st in results:
        station_dict = {}

        station_dict["id"] = st[0]
        station_dict["station"] = st[1]
        station_dict["name"] = st[2]

        list_stations.append(station_dict)

    # Return a JSON list of stations from the dataset.
    return jsonify(list_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Server reuested climate app temp observation data ...")

    # --- create a session from Python to the database ---
    session = Session(engine)

    # Query the dates and temperature observations of the most active station for the last year of data.

    # --- identify the most active station ---
    stats1 = session.query(measure.station,func.count(measure.station)).\
    group_by(measure.station).order_by(func.count(measure.station).desc()).first()

    # --- identify the last date, convert to datetime and calculate the start date (12 months from the last date) ---
    max_dt1 = session.query(func.max(measure.date)).filter(measure.station == stats1[0]).all()
    recent_dt= dt.datetime.strptime(max_dt1[0][0],"%Y-%m-%d").date()
    one_year_dt = recent_dt - dt.timedelta(days=365)

    # --- build query for tobs with above conditions ---
    qry = session.query(measure.tobs).filter(measure.date <= recent_dt).\
        filter(measure.date >= one_year_dt).filter(measure.station == stats1[0]).all()

    # --- close the session ---
    session.close()

    # --- create a list of dictionaries with station info using for loop---
    list_stations = []

    for st in qry:
        station_dict = {}

        station_dict["tobs"] = st[0]
        

        list_stations.append(station_dict)

    # Return a JSON list of temperature observations (TOBS) for the previous year.
    return jsonify(list_stations)

@app.route("/api/v1.0/<start>")
def temps_from_start(start):
    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.

    print(f"Server requested climate app daily normals from {start}...")

    # --- create a function to calculate the daily normals given a certain start date (datetime object in the format "%Y-%m-%d") ---
    def daily_normals(start_date):

        # --- create a session from Python to the database ---
        session = Session(engine)   

        sel = [measure.date, func.min(measure.tobs), func.avg(measure.tobs), func.max(measure.tobs)]
        qry1 =  session.query(*sel).filter(func.strftime("%Y-%m-%d", measure.date) >= func.strftime("%Y-%m-%d", start_date)).group_by(measure.date).all()

        # --- close the session ---
        session.close()   
        return qry1

    try:
        # --- convert the start date to a datetime object for validating and error handling ---
        start_date = dt.datetime.strptime(start, "%Y-%m-%d")

        # --- call the daily_normals function to calculate normals from the start date and save the result ---
        results = daily_normals(start_date)
        normals=[]

        # --- create a for loop to go through row and calculate daily normals ---
        for temp_date, tmin, tavg, tmax in results:

            # --- create an empty dictionary and store results for each row ---
            temps_dict = {}
            temps_dict["Date"] = temp_date
            temps_dict["T-Min"] = tmin
            temps_dict["T-Avg"] = tavg
            temps_dict["T-Max"] = tmax

            # --- append each result's dictionary to the normals list ---
            normals.append(temps_dict)

        # --- return the JSON list of normals ---
        return jsonify(normals)

    except ValueError:
        return "Please enter a start date in the format 'YYYY-MM-DD'"

@app.route("/api/v1.0/<start>/<end>")
def temps_between(start, end):
#When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.

    print(f"Server requested climate app daily normals from {start} to {end}...")

    # --- create a function to calculate the daily normals given certain start and end dates (datetime objects in the format "%Y-%m-%d") ---
    def daily_normals(start_date, end_date):

        # --- create a session from Python to the database ---
        session = Session(engine)   

        sel = [measure.date, func.min(measure.tobs), func.avg(measure.tobs), func.max(measure.tobs)]
        qry2 = session.query(*sel).filter(func.strftime("%Y-%m-%d", measure.date) >= func.strftime("%Y-%m-%d", start_date)).\
                                   filter(func.strftime("%Y-%m-%d", measure.date) <= func.strftime("%Y-%m-%d", end_date)).\
                                    group_by(measure.date).all()

        # --- close the session ---
        session.close()
        return qry2
        
    try:
        # --- convert the start date to a datetime object for validating and error handling ---
        start_date = dt.datetime.strptime(start, "%Y-%m-%d")
        end_date = dt.datetime.strptime(end, "%Y-%m-%d")

        # --- call the daily_normals function to calculate normals from the start date and save the result ---
        results = daily_normals(start_date, end_date)
        normals=[]

        # --- create a for loop to go through row and calculate daily normals ---
        for temp_date, tmin, tavg, tmax in results:

            # --- create an empty dictionary and store results for each row ---
            temps_dict = {}
            temps_dict["Date"] = temp_date
            temps_dict["T-Min"] = tmin
            temps_dict["T-Avg"] = tavg
            temps_dict["T-Max"] = tmax

            # --- append each result's dictionary to the normals list ---
            normals.append(temps_dict)

        # --- return the JSON list of normals ---
        return jsonify(normals)

    except ValueError:
        return "Please enter dates in the following order and format: 'start_date/end_date' i.e. 'YYYY-MM-DD'/'YYYY-MM-DD'"
    


if __name__ == "__main__":
    app.run(debug=True)
