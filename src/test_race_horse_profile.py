from main import app
from race_horse_profile import *
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
    DB.execute(conn, """TRUNCATE TABLE race_horse_profile""", ())


class first_race_horse_profile(BaseModel):
    race_date: Optional[str] = "1900-01-01"
    race_venue: Optional[str] = "HV"
    race_num: Optional[int] = 9
    horse_num: Optional[int] = 11
    draw_num: Optional[int] = 1
    horse_code: Optional[str] = "E203"
    horse_name: Optional[str] = "上駿之星"
    jockey_code: Optional[str] = "TEK"
    jockey_name: Optional[str] = "田泰安"
    trainer_code: Optional[str] = "SCS"
    trainer_name: Optional[str] = "沈集成"
    horse_weight: Optional[int] = 1303
    horse_handicap_weight: Optional[int] = 113
    horse_runner_rating: Optional[int] = 87
    horse_rating_change: Optional[str] = "+7"
    horse_gear: Optional[str] = ""
    saddle_cloth: Optional[str] = None
    standby_status: Optional[str] = "N"
    apprentice_allowance: Optional[str] = None
    scratched: Optional[bool] = False
    scratched_group: Optional[bool] = False
    members: Optional[str] = "{}"
    priority: Optional[str] = "N"
    trump_card: Optional[str] = "N"
    preference: Optional[str] = "1"


class updated_race_horse_profile(BaseModel):
    race_date: Optional[str] = "1900-01-01"
    race_venue: Optional[str] = "HV"
    race_num: Optional[int] = 9
    horse_num: Optional[int] = 11
    draw_num: Optional[int] = 1
    horse_code: Optional[str] = "E203"
    horse_name: Optional[str] = "上駿之星"
    jockey_code: Optional[str] = "ASD"
    jockey_name: Optional[str] = "田泰安"
    trainer_code: Optional[str] = "NQW"
    trainer_name: Optional[str] = "沈集成"
    horse_weight: Optional[int] = 1303
    horse_handicap_weight: Optional[int] = 1133 #modified
    horse_runner_rating: Optional[int] = 872 #modified
    horse_rating_change: Optional[str] = "+1" #modified
    horse_gear: Optional[str] = ""
    saddle_cloth: Optional[str] = None
    standby_status: Optional[str] = "N"
    apprentice_allowance: Optional[str] = None
    scratched: Optional[bool] = False
    scratched_group: Optional[bool] = False
    members: Optional[str] = "{}"
    priority: Optional[str] = "N"
    trump_card: Optional[str] = "N"
    preference: Optional[str] = "2" #modified


def BaseModel_to_json(race_horse_object):
    """
    This function converts an object to json format
    """
    return json.loads(race_horse_object.json())


def test_post_race_horse_profile():
    """
    This function tests the race_horse_profile POST API
    """
    first_horse_json = BaseModel_to_json(first_race_horse_profile())
    response = client.post("/api/v1/race_horse_profile/",json=first_horse_json)
    assert response.status_code == 200
    compare_two_objects(first_horse_json, response.json())


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
    This function checks a race profile exist or not
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert check_race_profile_existed(conn, first_race_horse_profile()) == False
        assert insert_horse(conn, first_race_horse_profile()) == True
        assert check_race_profile_existed(conn, first_race_horse_profile()) == True
        clean_db_table(conn)
        DB.release_connection(conn)


def test_race_profile_insert():
    """
    This function checks the db insert function
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert insert_horse(conn, first_race_horse_profile()) == True
        clean_db_table(conn)
        DB.release_connection(conn)


def test_race_profile_update():
    """
    This function checks the db update function
    """
    conn = test_db_connect()
    if conn != None:
        clean_db_table(conn)
        assert insert_horse(conn, first_race_horse_profile()) == True
        assert update_horse(conn, updated_race_horse_profile()) == True
        clean_db_table(conn)
        assert update_horse(conn, updated_race_horse_profile()) == False
        DB.release_connection(conn)