# Incident Data Parser and Database Storage

This repository contains a Python script that parses XML data from a specific URL and stores it in a PostgreSQL database. The code utilizes SQLAlchemy for the database connection and xml.sax for XML parsing.

## Prerequisites

Before running the code, make sure you have the following dependencies installed:

- Python 3.x
- SQLAlchemy
- xml.sax

You also need access to a PostgreSQL database with the appropriate credentials.

API: http://txdot-its-c2c.txdot.gov/XmlDataPortal_AUS/api/c2c?networks=AUS&dataTypes=incidentData

## Installation

1. Clone the repository:
git clone http://yk9253@gitlab.utnmc.org/yk9253/c2c_incidents.git


## Configuration

Make sure to configure the database connection details before running the code. Open the script file (`main.py`) and update the following variables:

```python
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD 
Update the above variables with your actual database connection details.

Usage
To run the script, navigate to the repository directory and execute the following command:
python main.py

The script will start parsing XML data from the specified URL and store it in the configured PostgreSQL database. The data is stored in three tables: c2c_incidents, c2c_lanes, and c2c_aff, representing incident data, lane details, and affected lanes, respectively.

The script will keep running indefinitely, parsing and updating the database every 30 seconds (or the specified UPDATE_TIME interval).

