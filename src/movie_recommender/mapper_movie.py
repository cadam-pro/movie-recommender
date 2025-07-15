from datetime import datetime
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

