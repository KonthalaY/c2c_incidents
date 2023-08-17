import time
import xml.sax
import logging
import os
import signal
import pathlib
import sys
import traceback
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Time, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#Traffic Incidents API
XML_URL = "http://txdot-its-c2c.txdot.gov/XmlDataPortal_AUS/api/c2c?networks=AUS&dataTypes=incidentData"

#Define the database connection
DB_HOST = 'nmc-compute2.ctr.utexas.edu'
DB_PORT = 5434
DB_NAME = 'perm_aus'
DB_USER = 'nmc'
DB_PASSWORD = 'nmc0864Q'

UPDATE_TIME = 30  # seconds

Base = declarative_base()

#Define the c2c_incidents table
class IncidentData(Base):
    __tablename__ = 'c2c_incidents2'

    uid = Column("uid", Integer, autoincrement=True, primary_key=True)
    id = Column("id", String)
    desc = Column("desc", String)
    roadway = Column("roadway", String)
    direction = Column("direction", String)
    crossstreet = Column("crossstreet", String)
    lat = Column("lat", String)
    lon = Column("lon", String)
    status = Column("status", String)
    updateType = Column("updateType", String)
    severity = Column("severity", String)
    eventType = Column("eventType", String)
    confirmedDate = Column("confirmedDate", DateTime)
    confirmedTime = Column("confirmedTime", Time)
    timestamp = Column('timestamp', DateTime)

# Define the c2c_inc_lanes_details table
class IncidentLaneDetail(Base):
    __tablename__ = 'c2c_lanes2'

    uid = Column("uid", Integer, primary_key=True)
    id = Column("id", String)
    typ = Column("type", String)
    status = Column("status", String)
    index = Column("index", Integer, primary_key=True)

# Define the c2c_inc_aff_lanes table
class IncidentAffectedLane(Base):
    __tablename__ = 'c2c_aff2'

    uid = Column("uid", Integer, primary_key=True)
    id = Column("id", String)
    key = Column("key", String, primary_key=True)
    value = Column("value", String)

def convert_date(date_str):
    try:
        # Check if the date is in the 'DD/MM/YYYY' format
        datetime_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return datetime_obj.date()

    except ValueError:
        # If the date is already in the 'DD-MM-YYYY' format, return it as is
        datetime_obj = datetime.strptime(date_str, '%m/%d/%Y')
        return datetime_obj.date()

_exitFlag = False
def receiveSignal(sig, stackFrame):
    """
    Signal handler for alerting that we should exit.
    """
    global _exitFlag
    if sig == signal.SIGINT:
        print("\nReceived SIGINT signal (Ctrl+C). Exiting gracefully...")
    elif sig == signal.SIGTERM:
        print("\nReceived SIGTERM signal (Ctrl+\). Exiting gracefully...")
    #print("Received exit signal. Will exit at the end of the current loop...")
    _exitFlag = True


class IncidentHandler(xml.sax.ContentHandler):
    def __init__(self, session):
        xml.sax.ContentHandler.__init__(self)
        self.session = session
        self.current_incident = None
        self.current_tag = ''
        self.incidents = []
        self.current_data = {}
        self.current_affected_lanes = None
        self.current_lanes_details = None
        self.ld = []
        self.temp = []

    def startElement(self, name, attrs):
        self.current_tag = name
        if self.current_tag == "incident":
            self.current_incident = IncidentData()
            self.current_incident.id = attrs['id']
            self.ld = []
        if self.current_tag == "affectedLanes":
            self.current_affected_lanes = IncidentAffectedLane()
            self.current_affected_lanes.id = self.current_incident.id
            self.temp = []
            if attrs.items():
                for attr_name, attr_value in attrs.items():
                    dup = [attr_name, attr_value]
                    self.temp.append(dup)
        if self.current_tag == "laneDetails":
            self.current_lanes_details = IncidentLaneDetail()
            self.current_lanes_details.id = self.current_incident.id
            lane = [attrs['type'], attrs['status'], attrs['index']]
            self.ld.append(lane)

    def characters(self, content):
        if self.current_tag in ('desc', 'roadway', 'direction', 'crossstreet', 'lat', 'lon', 'status', 'updateType',
                                'severity', 'eventType', 'confirmedDate', 'confirmedTime'):
            self.current_data[self.current_tag] = content


    def endElement(self, name):
        if name == 'incident':
            # Check if a record with the same id already exists
            existing_record = self.session.query(IncidentData).filter_by(id=self.current_incident.id).group_by(IncidentData.id, IncidentData.uid).order_by(desc(IncidentData.timestamp)).limit(1).all()
            if not existing_record:
                try:
                    self.current_incident.desc = self.current_data['desc']
                    self.current_incident.roadway = self.current_data['roadway']
                    self.current_incident.direction = self.current_data['direction']
                    self.current_incident.crossstreet = self.current_data['crossstreet']
                    self.current_incident.lat = self.current_data['lat']
                    self.current_incident.lon = self.current_data['lon']
                    self.current_incident.status = self.current_data['status']
                    self.current_incident.updateType = self.current_data['updateType']
                    self.current_incident.severity = self.current_data['severity']
                    self.current_incident.eventType = self.current_data['eventType']
                    self.current_incident.confirmedDate = convert_date(self.current_data['confirmedDate'])
                    self.current_incident.confirmedTime = self.current_data['confirmedTime']
                    self.current_incident.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    logging.info("---IncidentData---")
                    self.session.add(self.current_incident)
                    self.session.commit()
                    logging.info(f"Added data for incident ID: {self.current_incident.id}")
                    try:
                        logging.info("---lanesDetails---")
                        for idx in range(len(self.ld)):
                            self.current_lanes_details = IncidentLaneDetail()
                            self.current_lanes_details.uid = self.current_incident.uid
                            self.current_lanes_details.id = self.current_incident.id
                            self.current_lanes_details.typ = self.ld[idx][0]
                            self.current_lanes_details.status = self.ld[idx][1]
                            self.current_lanes_details.index = self.ld[idx][2]
                            self.session.add(self.current_lanes_details)
                            self.session.commit()
                            self.current_lane_details = None
                            logging.info(f"Added lane details for incident ID: {self.current_incident.id}")
                    except Exception as e:
                        self.session.rollback()
                        logging.error(f"Failed to add lane details for incident ID: {self.current_incident.id}")
                        logging.error(str(e))
                    logging.info("---affectedLanes---")
                    if len(self.temp)>0:
                        for idx in range(len(self.temp)):
                            self.current_affected_lanes = IncidentAffectedLane()
                            self.current_affected_lanes.uid = self.current_incident.uid
                            self.current_affected_lanes.id = self.current_incident.id
                            self.current_affected_lanes.key = self.temp[idx][0]
                            self.current_affected_lanes.value = self.temp[idx][1]
                            self.session.add(self.current_affected_lanes)
                            self.session.commit()
                            logging.info(f"Added affected lanes for incident ID: {self.current_incident.id}")
                            self.current_affected_lanes = None
                except Exception as e:
                    self.session.rollback()
                    logging.error(f"Failed to add data for incident ID: {self.current_incident.id}")
                    logging.error(str(e))
                self.current_incident = None
            else:
                logging.info("---Existing_record---")
                for record in existing_record:
                    uid = record.uid
                    lanedetails = []
                    l = False
                    a = False
                    i = False
                    existing_ld = self.session.query(IncidentLaneDetail).filter_by(uid=record.uid).all()
                    for ld in existing_ld:
                        temp = [ld.typ, ld.status, str(ld.index)]
                        lanedetails.append(temp)
                    if lanedetails != self.ld:  #different lane details
                        l = True
                    #print("different lane details?", l)
                    existing_al = self.session.query(IncidentAffectedLane).filter_by(uid=record.uid).all()
                    affectedLanes = []
                    for al in existing_al:
                        t = [al.key, al.value]
                        affectedLanes.append(t)
                    if affectedLanes != self.temp: #different affected lanes
                        a = True
                    #print("different affected lanes", a)
                    if record.desc != self.current_data['desc']:
                        #print(record.desc, self.current_data['desc'])
                        i = True
                    if record.roadway != self.current_data['roadway']:
                        #print(record.roadway, self.current_data['roadway'])
                        i = True
                    if record.direction != self.current_data['direction']:
                        #print(record.direction, self.current_data['direction'])
                        i = True
                    if record.crossstreet != self.current_data['crossstreet']:
                        #print(record.crossstreet, self.current_data['crossstreet'])
                        i = True
                    if record.lat != self.current_data['lat']:
                        #print(record.lat, self.current_data['lat'])
                        i = True
                    if record.lon != self.current_data['lon']:
                        #print(record.lon, self.current_data['lon'])
                        i = True
                    if record.status != self.current_data['status']:
                        #print(record.status, self.current_data['status'])
                        i = True
                    if record.updateType != self.current_data['updateType']:
                        #print(record.updateType, self.current_data['updateType'])
                        i = True
                    if record.severity != self.current_data['severity']:
                        #print(record.severity, self.current_data['severity'])
                        i = True
                    if record.eventType != self.current_data['eventType']:
                        #print(record.eventType, self.current_data['eventType'])
                        i =True
                    #print("different incident data?", i)
                    if a or l or i:
                        try:
                            self.current_incident.desc = self.current_data['desc']
                            self.current_incident.roadway = self.current_data['roadway']
                            self.current_incident.direction = self.current_data['direction']
                            self.current_incident.crossstreet = self.current_data['crossstreet']
                            self.current_incident.lat = self.current_data['lat']
                            self.current_incident.lon = self.current_data['lon']
                            self.current_incident.status = self.current_data['status']
                            self.current_incident.updateType = self.current_data['updateType']
                            self.current_incident.severity = self.current_data['severity']
                            self.current_incident.eventType = self.current_data['eventType']
                            self.current_incident.confirmedDate = convert_date(self.current_data['confirmedDate'])
                            self.current_incident.confirmedTime = self.current_data['confirmedTime']
                            self.current_incident.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            #print("---IncidentData---")
                            self.session.add(self.current_incident)
                            self.session.commit()
                            logging.info(f"Added updated data for incident ID: {self.current_incident.id}")
                            try:
                                logging.info("---lanesDetails---")
                                for idx in range(len(self.ld)):
                                    self.current_lanes_details = IncidentLaneDetail()
                                    self.current_lanes_details.uid = self.current_incident.uid
                                    self.current_lanes_details.id = self.current_incident.id
                                    self.current_lanes_details.typ = self.ld[idx][0]
                                    self.current_lanes_details.status = self.ld[idx][1]
                                    self.current_lanes_details.index = self.ld[idx][2]
                                    self.session.add(self.current_lanes_details)
                                    self.session.commit()
                                    self.current_lane_details = None
                                    logging.info(f"Added updated lane details for incident ID: {self.current_incident.id}")
                            except Exception as e:
                                self.session.rollback()
                                logging.error(f"Failed to add updated lane details for incident ID: {self.current_incident.id}")
                                logging.error(str(e))
                            logging.info("---affectedLanes---")
                            if len(self.temp)>0:
                                for idx in range(len(self.temp)):
                                    self.current_affected_lanes = IncidentAffectedLane()
                                    self.current_affected_lanes.uid = self.current_incident.uid
                                    self.current_affected_lanes.id = self.current_incident.id
                                    self.current_affected_lanes.key = self.temp[idx][0]
                                    self.current_affected_lanes.value = self.temp[idx][1]
                                    self.session.add(self.current_affected_lanes)
                                    self.session.commit()
                                    logging.info(f"Added updated affected lanes for incident ID: {self.current_incident.id}")
                                    self.current_affected_lanes = None
                        except Exception as e:
                            self.session.rollback()
                            logging.error(f"Failed to add updated data for incident ID: {self.current_incident.id}")
                            logging.error(str(e))
                        self.current_incident = None
                    else:
                        logging.info(f"no update in the record - {self.current_incident.id}")

def create_database():
    # Create the engine and connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    Base.metadata.create_all(engine)

    # Create the session
    Session = sessionmaker(bind=engine)
    session = Session()

    return session



def main():

    log_folder = "logs"
    pathlib.Path(log_folder).mkdir(exist_ok=True)
    file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"

    # Set log file path
    log_file_path = os.path.join(log_folder, file_name)

    # Set up logging
    logging.basicConfig(filename=log_file_path,level=logging.INFO, filemode='w', format='%(name)s - %(asctime)s - %(levelname)s - %(message)s')
    session = create_database()

    handler = IncidentHandler(session)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    #signal.signal(signal.SIGHUP, receiveSignal) # <-- This handles a typical "nice" exit request
    signal.signal(signal.SIGINT, receiveSignal) # <-- This handles CTRL-C
    signal.signal(signal.SIGTERM, receiveSignal) # <-- This handles CTRL-\ (quit + core dump)
    while True:
        try:
            if _exitFlag: # This is where we should safely exit.
                break
            logging.info("Parsing XML data...")
            parser.parse(XML_URL)
            logging.info("Parsing completed.")
        except Exception as e:
            logging.error("An error occurred during XML parsing.", str(e))
            logging.error(traceback.format_exc())
        time.sleep(UPDATE_TIME)

if __name__ == '__main__':
    main()


