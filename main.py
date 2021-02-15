#!/usr/bin/env python

from asyncio import get_event_loop, ensure_future, gather
from itertools import groupby
from json import load, dump, loads
from os import listdir
from time import monotonic

from aiohttp import ClientSession
from requests import get


def write_data_json(name_json, data_json):
    with open(name_json, "w") as j:
        dump(obj=data_json, fp=j, indent=2)


def get_beatmaps(user, api_key):
    url = "https://osu.ppy.sh/api/get_beatmaps?k={}&u={}"
    ready_url = url.format(api_key, user)
    return get(ready_url).json()


async def _fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def _run_check(total_rooms, room_id, api_key):
    url = "https://osu.ppy.sh/api/get_match?k={}&mp={}"
    tasks = []
    async with ClientSession() as session:
        for j in range(total_rooms):
            task = ensure_future(_fetch(url.format(api_key, room_id + j), session))
            tasks.append(task)
        return await gather(*tasks)


async def _run_recheck(rooms, api_key):
    url = "https://osu.ppy.sh/api/get_match?k={}&mp={}"
    tasks = []
    async with ClientSession() as session:
        for j in rooms:
            task = ensure_future(_fetch(url.format(api_key, j["match_id"]), session))
            tasks.append(task)
        return await gather(*tasks)


def _check_new_open_rooms(rooms):
    open_rooms = []
    for room in rooms:
        if room["match"] != 0 and room["match"]["end_time"] is None:
            if room["games"]:
                beatmap_id = room["games"][len(room["games"]) - 1]["beatmap_id"]
            else:
                beatmap_id = ""
            open_rooms.append({
                "match_id": room["match"]["match_id"],
                "name": room["match"]["name"],
                "beatmap_id": "{}".format(beatmap_id)
            })
    return open_rooms


def _check_open_rooms_debug(rooms):
    for room in rooms:
        if room["match"] == 0:
            print("Не существует")
        elif not room["games"]:
            if room["match"]["end_time"] is None:
                print("Жива и ничего не сыграли.")
            else:
                print("Умерла и ничего не сыграли")
        elif room["match"]["end_time"] is None:
            print("Живая комната")
        else:
            print("Мертва комната")


def _my_maps_in_open_rooms(my_maps, open_rooms):
    crossing = []
    for y in my_maps:
        for x in open_rooms:
            if y["beatmap_id"] == x["beatmap_id"]:
                crossing.append({
                    "artist": y["artist"],
                    "title": y["title"],
                    "version": y["version"],
                    "name": x["name"]
                })
    return crossing


def _check_my_own_maps(my_maps_data, user_id, api_key):
    if not my_maps_data:
        my_maps = []
        data = get_beatmaps(user_id, api_key)
        for j in data:
            new_dict = {
                "artist": j["artist"],
                "title": j["title"],
                "version": j["version"],
                "beatmap_id": j["beatmap_id"]
            }
            my_maps.append(new_dict)
        my_maps_data = my_maps
        write_data_json("my_maps.json", my_maps)
    return my_maps_data


def _check_open_rooms(open_rooms, api_key):
    start_time = monotonic()
    open_rooms_recheck = ensure_future(_run_recheck(open_rooms, api_key))
    server_data = [loads(j) for j in get_event_loop().run_until_complete(open_rooms_recheck)]
    print('Время проверки открытых комнат: {:>.2f} сек.'.format(monotonic() - start_time), "\n")
    return _check_new_open_rooms(server_data)


def _check_limit(rooms):
    not_exists = [True if room["match"] == 0 else False for room in rooms]
    groups = groupby(not_exists)
    occurrences_of_negatives = [len(list(g)) for k, g in groups if k]
    if occurrences_of_negatives:
        # return max(occurrences_of_negatives)
        return occurrences_of_negatives[0]
    return 0


def check_personal_data(personaly_data):
    if not (personaly_data["api_key"] and personaly_data["user_id"]):
        print("Заполните personal_data.json")
        exit()
    return personaly_data


def main():
    json_file = {file[:-5]: load(open(file)) for file in listdir() if '.json' in file}
    personal_data = check_personal_data(json_file["personal_data"])
    my_maps = _check_my_own_maps(json_file["my_maps"], personal_data["user_id"], personal_data["api_key"])
    open_rooms = _check_open_rooms(json_file["open_rooms"], personal_data["api_key"])
    pool_and_room_id = json_file["pool_and_room_id"]

    start_time = monotonic()
    future = ensure_future(_run_check(pool_and_room_id["pool"], pool_and_room_id["room_id"], personal_data["api_key"]))
    server_data = [loads(i) for i in get_event_loop().run_until_complete(future)]
    print('Время обработки новых запросов: {:>.2f} сек'.format(monotonic() - start_time), "\n")

    limit = _check_limit(server_data)
    new_open_rooms = _check_new_open_rooms(server_data)
    open_rooms += new_open_rooms
    open_rooms = list({i['match_id']: i for i in open_rooms}.values())
    write_data_json("open_rooms.json", open_rooms)

    # check_open_rooms_debug(server_data)
    pool_and_room_id["room_id"] += pool_and_room_id["pool"]
    if limit > 10:
        pool_and_room_id["room_id"] -= limit
    write_data_json("pool_and_room_id.json", pool_and_room_id)
    return _my_maps_in_open_rooms(my_maps, open_rooms)


if __name__ == '__main__':
    print(main())
