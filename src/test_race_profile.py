from main import app
from race_profile import *
from fastapi.testclient import TestClient
from pydantic import BaseModel
import json

client = TestClient(app)

def compare_two_objects(object, test_object):
    """
    This function s two objects' values
    """
    for key in object:
        assert object[key] == test_object[key]


def clean_db_table(conn):
    """
    This function truncates the table
    """
    DB.execute(conn, """TRUNCATE TABLE race_profile""", ())


class first_race_profile(BaseModel):
    race_date: Optional[str] = "1900-01-01"
    race_venue: Optional[str] = "ST"
    race_num: Optional[int] = 1
    race_name: Optional[str] = "禾輋讓賽"
    race_time: Optional[str] = "20220227174500"
    race_prize: Optional[int] = 1234567
    race_class: Optional[str] = "第五班"
    race_ground: Optional[str] = "全天候跑道"
    race_track: Optional[str] = "A"
    race_length: Optional[int] = 1234
    race_ground_condition: Optional[str] = "好地"


class updated_race_profile(BaseModel):
    race_date: Optional[str] = "1900-01-01"
    race_venue: Optional[str] = "ST"
    race_num: Optional[int] = 1
    race_name: Optional[str] = "禾輋讓賽"
    race_time: Optional[str] = "20220227174500"
    race_prize: Optional[int] = 98765431 #modified
    race_class: Optional[str] = "第一班" #modified
    race_ground: Optional[str] = "全天候跑道"
    race_track: Optional[str] = "B" #modified
    race_length: Optional[int] = 2000 #modified
    race_ground_condition: Optional[str] = "好地"


def BaseModel_to_json(race_object):
    """
    This function converts an object to json format
    """
    return json.loads(race_object.json())


def test_post_race_horse_profile():
    """
    This function tests the race_horse_profile POST API
    """
    first_race_json = BaseModel_to_json(first_race_profile())
    response = client.post("/api/v1/race_profile/",json= first_race_json)
    assert response.status_code == 200
    compare_two_objects(first_race_json, response.json())


def test_db_connect():
    """
    This function checks the DB connection
    """
    conn = None
    if conn is None:
        conn = check_conn()
    assert conn != None
    return conn


def test_race_profile_existed():
    """
    This function checks a race horse profile exist or not
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert check_race_profile_existed(conn, first_race_profile()) == False
        assert insert_horse(conn, first_race_profile()) == True
        assert check_race_profile_existed(conn, first_race_profile()) == True
        clean_db_table(conn)
        DB.release_connection(conn)


def test_race_profile_insert():
    """
    This function checks the db insert function
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert insert_horse(conn, first_race_profile()) == True
        clean_db_table(conn)
        DB.release_connection(conn)


def test_race_profile_update():
    """
    This function checks the db update function
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert insert_horse(conn, first_race_profile()) == True
        assert update_race(conn, updated_race_profile()) == True
        clean_db_table(conn)
        assert update_race(conn, updated_race_profile()) == False
        DB.release_connection(conn)