import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import DbSession, SwapiPeople, close_orm, init_orm

MAX_CHUNK = 5


async def get_people(http_session, people_id):
    response = await http_session.get(f"https://swapi.dev/api/people/{people_id}/")
    json_data = await response.json()
    return json_data


async def insert_people(json_list):
    async with DbSession() as session:
        orm_objects = []
        for data in json_list:
            orm_objects.append(
                SwapiPeople(
                    birth_year=data.get('birth_year', 'unknown'),
                    eye_color=data.get('eye_color', 'unknown'),
                    films=', '.join(data.get('films', [])),
                    gender=data.get('gender', 'unknown'),
                    hair_color=data.get('hair_color', 'unknown'),
                    height=data.get('height', 'unknown'),
                    homeworld=data.get('homeworld', 'unknown'),
                    mass=data.get('mass', 'unknown'),
                    name=data.get('name', 'unknown'),
                    skin_color=data.get('skin_color', 'unknown'),
                    species=', '.join(data.get('species', [])),
                    starships=', '.join(data.get('starships', [])),
                    vehicles=', '.join(data.get('vehicles', [])),
                )
            )
        session.add_all(orm_objects)
        await session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for chunk_i in chunked(range(1, 100), MAX_CHUNK):
            coros = [get_people(http_session, i) for i in chunk_i]
            result = await asyncio.gather(*coros)
            await insert_people(result)
    await close_orm()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
