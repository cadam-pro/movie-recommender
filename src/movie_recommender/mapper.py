from datetime import datetime
from http_requester import get_credit
import json

def update_release_date(change):
    if "value" in change:
        return datetime.strptime(change.get("value").get("release_date", {}), '%Y-%m-%d').date()
    else:
        return datetime.strptime(change.get("original_value").get("release_date", {}) , '%Y-%m-%d').date()
    
def update_tagline(change):
    for item in change:
        if "value" in item:
            return item.get("value").get("tagline")
        else:
            return item.get("original_value").get("tagline")   

def update_production_company(change):
    added = []
    for item in change:
        if item.get("action") == "added":
            added.append(item.get("value").get("name"))
    return ",".join(added)


def update_cast(change, credits, row, columns):
    added = []
    cast = credits.get("cast")
    for actor in cast:
        added.append(actor.get("original_name"))
    row[columns.index(change.get("key"))] = ",".join(added)
    return row

def update_crew(change, credits, row, columns):
    added_producer = []
    added_writer = []
    crew = credits.get("crew")
    for member in crew:
        job = member.get("job")
        match job:
            case "Director":
                row[columns.index("director")] = member.get("name")
            case "Original Music Composer":
                row[columns.index("music_composer")] = member.get("name")
            case "Director of Photography":
                row[columns.index("director_of_photography")] = member.get("name")
            case "Writer":
                added_writer.append(member.get("name"))
            case "Executive Producer":
                added_producer.append(member.get("name"))
            case "Producer":
                added_producer.append(member.get("name"))
            case _:
                pass
    row[columns.index("writers")] = ",".join(added_writer)
    row[columns.index("producers")] = ",".join(added_producer)
    return row

def update_genre(change):
    added = []
    for item in change:
        if item.get("acion") == "added":
            added.append(item.get("value").get("name"))
    return ",".join(added)


def update_movie(changes, row):
    empty = False
    columns = [
        "id", "title", "vote_average", "vote_count", "status", "release_dates", "revenue", "runtime",
        "budget", "imdb_id", "original_language", "original_title", "overview", "popularity",
        "tagline", "genres", "production_companies", "production_countries", "spoken_languages",
        "cast", "director", "director_of_photography", "writers", "producers", "music_composer",
        "imdb_rating", "imdb_votes", "poster_path"
    ]
    credits = []
    for change in changes:
        if change.get("key") != "status" :
            if (change.get("key") in columns) or (change.get("key") == "crew"):
                match change.get("key"):
                    case "release_dates":
                        row[columns.index(change.get("key"))] = update_release_date(change.get("items")[0])
                    case "tagline":
                        row[columns.index(change.get("key"))] = update_tagline(change.get("items"))
                    case "cast":
                        if credits == []:
                            credits = get_credit(row[0])
                            row = update_cast(change, credits, row, columns)
                    case "crew":
                        if credits == []:
                            credits = get_credit(row[0])
                            row = update_crew(change, credits, row, columns)
                            pass
                    case "production_companies":
                        row[columns.index(change.get("key"))] = update_production_company(change.get("items"))
                    case "genres":
                        row[columns.index(change.get("key"))] = update_genre(change.get("items"))
                    case _:
                        value = change.get("items")[0].get("value")
                        if isinstance(value, int):
                            value = float(value)
                        row[columns.index(change.get("key"))] = value   
    return row, empty


def map_cast(data):
    cast = []
    json_data = json.loads(data)
    for people in json_data.get("cast"):
        cast.append(people.get("name"))
    return ','.join(cast)
    

def map_crew(data, job):
    crew = []
    json_data = json.loads(data)
    for people in json_data.get("crew"):
         if people.get("job") in job:
            crew.append(people.get("name"))
    return ','.join(crew)
