from typing import Optional
import traceback
from pydantic import BaseModel
from db import DB
from logger import get_logger
from messaging.publisher import Publisher
import json

def check_race_profile_existed(conn,request):
    return 1 == DB.query(conn,
                         "SELECT COUNT(1) FROM race_horse_profile WHERE race_date = to_date(%s, 'YYYY-MM-DD') AND race_num = %s AND horse_code = %s",
                         (request.race_date, request.race_num, request.horse_code), 'one')

def check_conn():
    return DB.get_connection()

def insert_horse(conn, request):
    return 1 == DB.execute(conn, """
                        INSERT INTO race_horse_profile (race_date, race_venue, race_num, horse_num, draw_num, horse_code, 
                        horse_name, jockey_code, jockey_name, trainer_code, trainer_name, horse_weight, horse_handicap_weight, 
                        horse_runner_rating,horse_rating_change, horse_gear, saddle_cloth, standby_status, apprentice_allowance, scratched, 
                        scratched_group, members, priority, trump_card, preference, update_ts) 
                        VALUES (to_date(%s, 'YYYY-MM-DD'), %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);""",
                     (request.race_date, request.race_venue, request.race_num, request.horse_num, request.draw_num,
                      request.horse_code, request.horse_name, request.jockey_code, request.jockey_name, request.trainer_code,
                      request.trainer_name, request.horse_weight, request.horse_handicap_weight, request.horse_runner_rating, request.horse_rating_change,request.horse_gear,
                      request.saddle_cloth, request.standby_status, request.apprentice_allowance, request.scratched,
                      request.scratched_group, request.members, request.priority, request.trump_card, request.preference))

def update_horse(conn, request):
    return 1 == DB.execute(conn, """
                    UPDATE race_horse_profile SET horse_num = %s, draw_num = %s, jockey_code = %s, jockey_name = %s, trainer_code = %s, 
                    trainer_name = %s, horse_weight = %s, horse_handicap_weight = %s, 
                    horse_runner_rating = %s, horse_rating_change = %s, horse_gear = %s, saddle_cloth = %s, standby_status = %s, apprentice_allowance = %s, scratched = %s, 
                    scratched_group = %s, members = %s, priority = %s, trump_card = %s, preference = %s, update_ts = CURRENT_TIMESTAMP
                    WHERE horse_code = %s AND race_date = %s;""",
                    (request.horse_num, request.draw_num,
                     request.jockey_code, request.jockey_name, request.trainer_code,
                     request.trainer_name, request.horse_weight, request.horse_handicap_weight,
                     request.horse_runner_rating, request.horse_rating_change, request.horse_gear,
                     request.saddle_cloth, request.standby_status, request.apprentice_allowance, request.scratched,
                     request.scratched_group, request.members, request.priority, request.trump_card, request.preference,
                     request.horse_code, request.race_date))

class Race_horse_profile(BaseModel):

    race_date: Optional[str]
    race_venue: Optional[str]
    race_num: Optional[int]
    horse_num: Optional[int]
    draw_num: Optional[int]
    horse_code: Optional[str]
    horse_name: Optional[str]
    jockey_code: Optional[str]
    jockey_name: Optional[str]
    trainer_code: Optional[str]
    trainer_name: Optional[str]
    horse_weight: Optional[int]
    horse_handicap_weight: Optional[int]
    horse_runner_rating: Optional[int]
    horse_rating_change: Optional[str]
    horse_gear: Optional[str]
    saddle_cloth: Optional[str]
    standby_status: Optional[str]
    apprentice_allowance: Optional[str]
    scratched: Optional[bool]
    scratched_group: Optional[bool]
    members: Optional[str]
    priority: Optional[str]
    trump_card: Optional[str]
    preference: Optional[str]

    class Config:
        arbitrary_types_allowed = True


    def persist(request):
        conn = None
        race_horse_profile_inserted = False
        logger = get_logger(__name__)
        publisher = Publisher()

        try:
            publisher.send(f'ex/hkjc/browser-crawler/race-horse-updated/1/{request.race_date}/{request.race_num}', str(json.loads(request.json())), ())
        except Exception as exp:
            logger.error('[{}, {}, {}, {}] Error when publishing race horse event, try again.'.format(request.race_date, request.race_venue, request.race_num))
            logger.error('[{}, {}, {}, {}] {}: {}'.format(request.race_date, request.race_venue,request.race_num, type(exp), str(exp)))


        while not race_horse_profile_inserted:
            try:
                if conn is None:
                    conn = check_conn()

                race_profile_existed = check_race_profile_existed(conn, request)

                if race_profile_existed:
                    race_horse_profile_inserted = update_horse(conn, request)

                if not race_profile_existed:
                    race_horse_profile_inserted = insert_horse(conn, request)


                    if race_horse_profile_inserted:
                        logger.info('[{}, {}, {}, {}] Race horse profile is persisted.'.format(request.race_date, request.race_venue, request.race_num, request.horse_num))
                else:
                    race_horse_profile_inserted = True
                    logger.info('[{}, {}, {}, {}] Race horse profile is already in DB, update this race horse profile.'.format(request.race_date, request.race_venue, request.race_num, request.horse_num))
            except Exception as exp:
                logger.error('[{}, {}, {}, {}] Error when persisting race horse profile, try again.'.format(request.race_date, request.race_venue, request.race_num, request.horse_num))
                logger.error('[{}, {}, {}, {}] {}: {}'.format(request.race_date, request.race_venue, request.race_num, request.horse_num, type(exp), str(exp)))
                logger.error('[{}, {}, {}] {}'.format(request.race_date, request.race_venue, request.race_num, request.horse_num, traceback.format_exc()))

                if conn is not None:
                    try:
                        DB.release_connection(conn)
                    except Exception as exp:
                        logger.error('[{}, {}, {}, {}] Error when releasing connection due to errors.'.format(request.race_date, request.race_venue, request.race_num, request.horse_num))
                        logger.error('[{}, {}, {}, {}] {}: {}'.format(request.race_date, request.race_venue, request.race_num, request.horse_num, type(exp), str(exp)))
                        logger.error('[{}, {}, {}] {}'.format(request.race_date, request.race_venue, request.race_num, request.horse_num, traceback.format_exc()))
                    finally:
                        conn = None

        while logger.handlers:
            logger.handlers.clear()