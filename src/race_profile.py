import traceback
from logger import get_logger
from typing import Optional
from pydantic import BaseModel
from db import DB
from messaging.publisher import Publisher
import json


def check_race_profile_existed(conn,request):
    return 1 == DB.query(conn,
                  "SELECT COUNT(1) FROM race_profile WHERE race_date = to_date(%s, 'YYYY-MM-DD') AND race_num = %s",
                  (request.race_date, request.race_num), 'one')


def check_conn():
    return DB.get_connection()


def insert_horse(conn, request):
    return 1 == DB.execute(conn, """
                                INSERT INTO race_profile (race_date, race_venue, race_num, race_name, race_time, race_prize, race_class, 
                                race_ground, race_track, race_length, race_ground_condition, update_ts) 
                                VALUES (to_date(%s, 'YYYY-MM-DD'), %s, %s, %s, to_timestamp(%s, 'YYYYMMDDhh24miss') at time zone 'Asia/Hong_Kong', 
                                %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);""",
                               (request.race_date, request.race_venue, request.race_num,
                                request.race_name, request.race_time,request.race_prize,
                                request.race_class, request.race_ground,
                                request.race_track, request.race_length,
                                request.race_ground_condition)
                               )


def update_race(conn, request):
    return 1 == DB.execute(conn, """
                                UPDATE race_profile SET race_venue = %s, race_name  = %s, race_time  = to_timestamp(%s, 'YYYYMMDDhh24miss') at time zone 'Asia/Hong_Kong'
                                , race_prize  = %s, race_class = %s, 
                                race_ground = %s, race_track = %s, race_length = %s, race_ground_condition = %s, update_ts = CURRENT_TIMESTAMP
                                Where race_date = to_date(%s, 'YYYY-MM-DD') AND race_num = %s""",
                               ( request.race_venue,
                                request.race_name, request.race_time,request.race_prize,
                                request.race_class, request.race_ground,
                                request.race_track, request.race_length,
                                request.race_ground_condition, request.race_date, request.race_num)
                               )


class Race_profile(BaseModel):
    race_date: Optional[str]
    race_venue: Optional[str]
    race_num: Optional[int]
    race_name: Optional[str]
    race_time: Optional[str]
    race_prize: Optional[int]
    race_class: Optional[str]
    race_ground: Optional[str]
    race_track: Optional[str]
    race_length: Optional[int]
    race_ground_condition: Optional[str]

    class Config:
        arbitrary_types_allowed = True


    def persist(request):
        logger = get_logger(__name__)
        conn = None
        race_profile_inserted = False
        publisher = Publisher()


        try:
            publisher.send(f'ex/hkjc/browser-crawler/race-updated/1/{request.race_date}/{request.race_num}',str(json.loads(request.json())), ())
        except Exception as exp:
            logger.error('[{}, {}, {}, {}] Error when publishing race event, try again.'.format(request.race_date, request.race_venue, request.race_num))
            logger.error('[{}, {}, {}, {}] {}: {}'.format(request.race_date, request.race_venue,request.race_num, type(exp), str(exp)))


        while not race_profile_inserted:
            try:
                if conn is None:
                    conn = check_conn()

                race_profile_existed = check_race_profile_existed(conn, request)

                if not race_profile_existed:
                    race_profile_inserted = insert_horse(conn, request)
                if race_profile_existed:
                    race_profile_inserted = update_race(conn, request)

                    if race_profile_inserted:
                        logger.info('[{}, {}, {}] Race main profile is persisted.'.format(request.race_date,request.race_venue,request.race_num))
                else:
                    race_profile_inserted = True
                    logger.info('[{}, {}, {}] Race main profile is already in DB, update the  race profile.'.format(request.race_date, request.race_venue, request.race_num))

            except Exception as exp:
                logger.error('[{}, {}, {}] Error when persisting race main profile, try again.'.format(request.race_date,request.race_venue,request.race_num))
                logger.error('[{}, {}, {}] {}: {}'.format(request.race_date, request.race_venue,request.race_num, type(exp),str(exp)))
                logger.error('[{}, {}, {}] {}'.format(request.race_date, request.race_venue,request.race_num,traceback.format_exc()))


                if conn is not None:
                    try:
                        DB.release_connection(conn)
                    except Exception as exp:
                        logger.error('[{}, {}, {}] Error when releasing connection due to errors.'.format(request.race_date, request.race_num, request.race_num))
                        logger.error('[{}, {}, {}] {}: {}'.format(request.race_date, request.race_venue, request.race_num, type(exp), str(exp)))
                        logger.error('[{}, {}, {}] {}'.format(request.race_date, request.race_venue, request.race_num, traceback.format_exc()))
                    finally:
                        conn = None
        while logger.handlers:
            logger.handlers.clear()