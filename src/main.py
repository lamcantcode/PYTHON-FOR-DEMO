import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from race_profile import Race_profile
from pool import Pool
from win_place import Win_place
from logger import logging
from config import uvicorn_Settings
from race_horse_profile import Race_horse_profile
"""from quinella_place_odds import quinella_place_odds
from quinella_odds import quinella_odds
from today_results_race import Today_results_race
from today_results_race_horse import Today_results_race_horse
from forecast_rate import forecast_rate
from double_odds import double_rate"""

app = FastAPI()

origins = [
    "http://127.0.0.1:8000/",
    "https://bet.hkjc.com",
    "https://racing.hkjc.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"Welcome to race service"}


@app.post("/api/v1/total_investemnt_pool/")
async def save_total_amount(request: Pool):
    Pool.persist(request)
    return request


@app.post("/api/v1/win_place_rate/")
async def save_win_rate(request: Win_place):
    Win_place.persist(request)
    return request


@app.post("/api/v1/place_odds/")
async def save_place_odds(request: Win_place):
    Win_place.persist(request)
    return request


@app.post("/api/v1/race_horse_profile/")
async def save_race_horse_profile(request: Race_horse_profile):
    if (request.race_date != '1900-01-01'):
        Race_horse_profile.persist(request)
    return request


@app.post("/api/v1/race_profile/")
async def save_race_profile(request: Race_profile):
    if (request.race_date != '1900-01-01'):
        Race_profile.persist(request)
    return request


@app.post("/api/v1/quinella_place_rate/")
async def get_quinella_place_rate(request: quinella_place_odds):
    message = quinella_place_odds.persist(request)
    return message


@app.post("/api/v1/quinella_rate/")
async def get_quinella_rate(request: quinella_odds):
    message = quinella_odds.persist(request)
    return message


@app.post("/api/v1/forecast_rate/")
async def get_forecast_rate(request: forecast_rate):
    message = forecast_rate.persist(request)
    return message


@app.post("/api/v1/double_rate/")
async def get_double_rate(request: double_odds):
    message = double_odds.persist(request)
    return message


@app.post("/api/v1/real_time_race_info")
async def get_real_time_race_info(request: Today_results_race):
    message = Today_results_race.persist(request)
    return message


@app.post("/api/v1/real_time_race_horse_info")
async def get_real_time_race_horse_info(request: Today_results_race_horse):
    message = Today_results_race_horse.persist(request)
    return message


if __name__ == "__main__":
    config = uvicorn_Settings(_env_file="../config/settings.env", )
    uvicorn.run("main:app",
                host=config.HOST,
                port=config.PORT,
                log_level=config.LOG_LEVEL,
                access_log=config.ACCESS_LOG,
                log_config=logging.basicConfig(
                    filename=config.LOG_FILENAME,
                    filemode=config.LOG_FILEMODE,
                    level=config.LOG_CONFIG_LEVEL,
                    format=config.LOG_FORMAT)
                )
