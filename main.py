from asyncio import get_event_loop, ensure_future, gather
from itertools import groupby
from json import dump, load, loads
from os import listdir
from time import monotonic

from aiohttp import ClientSession
from requests import get


def write_data_json(name_json, data_json):
    with open(name_json, "w") as j:
        dump(obj=data_json, fp=j, indent=2)


def get_beatmaps():
    url = "https://osu.ppy.sh/api/get_beatmaps?k={}&u={}"
    ready_url = url.format(personalData["api_key"], personalData["user_id"])
    return get(ready_url).json()


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def run_check(total_rooms, last_room):
    url = "https://osu.ppy.sh/api/get_match?k={}&mp={}"
    tasks = []
    async with ClientSession() as session:
        for j in range(total_rooms):
            task = ensure_future(fetch(url.format(personalData["api_key"], last_room + j), session))
            tasks.append(task)
        return await gather(*tasks)


async def run_recheck(rooms):
    url = "https://osu.ppy.sh/api/get_match?k={}&mp={}"
    tasks = []
    async with ClientSession() as session:
        for j in rooms:
            task = ensure_future(fetch(url.format(personalData["api_key"], j["match_id"]), session))
            tasks.append(task)
        return await gather(*tasks)


def check_new_open_rooms(rooms):
    open_rooms = []
    for room in rooms:
        if room["match"] != 0 and room["match"]["end_time"] is None:
            if room["games"]:
                beatmap_id = room["games"][0]["beatmap_id"]
            else:
                beatmap_id = ""
            open_rooms.append({
                "match_id": room["match"]["match_id"],
                "name": room["match"]["name"],
                "beatmap_id": "{}".format(beatmap_id)
            })
    return open_rooms


def check_open_rooms_debug(rooms):
    for room in rooms:
        if room["match"] == 0:  # Существует ли комната?
            print("Не существует")  # Не существует
        elif not room["games"]:  # Существует. Есть ли история игр?
            if room["match"]["end_time"] is None:  # И жива ли комната?
                print("Жива и ничего не сыграли.")  # Cуществует, нет история, жива
            else:
                print("Умерла и ничего не сыграли")  # Cуществует, нет история, мертва
        elif room["match"]["end_time"] is None:  # Существует и есть история. Она жива?
            print("Живая комната")  # Существует, есть история и жива
        else:
            print("Мертва комната")  # Существует, есть история и мертва


def my_maps_in_open_rooms(my_maps, open_rooms):
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


def check_my_own_maps(my_maps_data):
    if not my_maps_data:
        my_maps = []
        data = get_beatmaps()
        for j in data:
            new_dict = {
                "artist": j["artist"],
                "title": j["title"],
                "version": j["version"],
                "beatmap_id": j["beatmap_id"]
            }
            my_maps.append(new_dict)
        my_maps_data = my_maps
        write_data_json("myMaps.json", my_maps)
    return my_maps_data


def check_open_rooms(open_rooms):
    open_rooms_recheck = ensure_future(run_recheck(open_rooms))
    server_data = [loads(j) for j in get_event_loop().run_until_complete(open_rooms_recheck)]
    return check_new_open_rooms(server_data)


def check_limit(rooms):
    not_exists = [True if room["match"] == 0 else False for room in rooms]
    groups = groupby(not_exists)
    occurrences_of_negatives = [len(list(g)) for k, g in groups if k]
    if occurrences_of_negatives:
        # return max(occurrences_of_negatives)
        return occurrences_of_negatives[0]
    return 0


def check_personal_data(personal_data):
    if not (personal_data["api_key"] and personal_data["user_id"]):
        print("Заполните personalData.json")
        exit()
    return personal_data


jsonFile = {
    file[:-5]: load(open(file))
    for file in listdir()
    if '.json' in file
}

personalData = check_personal_data(jsonFile["personalData"])
myMaps = check_my_own_maps(jsonFile["myMaps"])
openRooms = check_open_rooms(jsonFile["openRooms"])
poolAndRoomId = jsonFile["poolAndRoomId"]

StartTime = monotonic()
future = ensure_future(run_check(poolAndRoomId["pool"], poolAndRoomId["last_room"]))
serverData = [loads(i) for i in get_event_loop().run_until_complete(future)]
print('Время обработки запросов: {:>.3f}'.format(monotonic() - StartTime), "\n")

newOpenRooms = check_new_open_rooms(serverData)
openRooms += newOpenRooms
openRooms = list({i['match_id']: i for i in openRooms}.values())
write_data_json("openRooms.json", openRooms)

# check_open_rooms_debug(serverData)

if check_limit(serverData) < 10:
    poolAndRoomId["last_room"] = poolAndRoomId["last_room"] + poolAndRoomId["pool"]
write_data_json("poolAndRoomId.json", poolAndRoomId)

print(my_maps_in_open_rooms(myMaps, openRooms))
