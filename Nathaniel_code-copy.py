from prefect import flow, task, tags
from prefect.logging import get_run_logger
import httpx
from prefect.tasks import task_input_hash
from typing import List


def fetch_weather(lat: float, lon: float):
    logger = get_run_logger()
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    logger.info(f"Most recent temp C: {most_recent_temp} degrees")
    return most_recent_temp


@task(name="fetch_many", cache_key_fn=task_input_hash, log_prints=True)
def fetch_many_weather(lat:float):
    lon = -20
    temp_list = {}
    while lon < 20:
        temp = fetch_weather(lat, lon)
        temp_list[lon] = temp
        lon += 5
    return temp_list


@task(name="save_weather_info", log_prints=True)
def save_weather(temp: List[float]):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"


@task(name="display", log_prints=True)
def display(temp_list):
    logger = get_run_logger()
    logger.info(temp_list)


@flow(name="weather_pipeline", retries=3)
def pipeline(lat: float = 42.7):
    temp = fetch_many_weather(lat)
    result = save_weather(temp)
    display(temp)
    return result


if __name__ == "__main__":
    pipeline(38.9)