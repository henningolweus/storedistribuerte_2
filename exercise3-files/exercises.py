from pymongo import MongoClient
from pprint import pprint
from datetime import timedelta, datetime
from haversine import haversine, Unit
from DbConnector import DbConnector

# Connect to the database
db_connector = DbConnector()
db = db_connector.db

# Task 1: Count number of users, activities, and trackpoints
def task1():
    num_users = db.User.count_documents({})
    num_activities = db.Activity.count_documents({})
    num_trackpoints = db.TrackPoint.count_documents({})
    pprint({"Users": num_users, "Activities": num_activities, "Trackpoints": num_trackpoints})

# Task 2: Find the average number of activities per user
def task2():
    pipeline = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$group": {"_id": None, "avg_activities_per_user": {"$avg": "$activity_count"}}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result[0]["avg_activities_per_user"] if result else "No data available")

# Task 3: Find the top 20 users with the highest number of activities
def task3():
    pipeline = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result)

# Task 4: Find all users who have taken a taxi
def task4():
    pipeline = [
        {"$match": {"transportation_mode": "taxi"}},
        {"$group": {"_id": "$user_id"}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint([user["_id"] for user in result])

# Task 5: Count transportation modes and activities tagged with them
def task5():
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result)

# Task 6a: Find the year with the most activities
def task6a():
    pipeline = [
        {"$project": {"year": {"$year": "$start_date_time"}}},
        {"$group": {"_id": "$year", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 1}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result[0] if result else "No data available")

# Task 6b: Find the year with the most recorded hours
def task6b():
    pipeline = [
        {"$project": {"year": {"$year": "$start_date_time"}, "duration": {"$subtract": ["$end_date_time", "$start_date_time"]}}},
        {"$group": {"_id": "$year", "total_hours": {"$sum": {"$divide": ["$duration", 3600000]}}}},  # Convert milliseconds to hours
        {"$sort": {"total_hours": -1}},
        {"$limit": 1}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result[0] if result else "No data available")


from haversine import haversine, Unit

def task7():
    user_id = "112"
    total_distance = 0

    # Find all activities for the user where the mode is 'walk' and year is 2008
    activities = db.Activity.find({"user_id": user_id, "transportation_mode": "walk", 
                                   "start_date_time": {"$gte": datetime(2008, 1, 1), "$lt": datetime(2009, 1, 1)}},
                                  {"_id": 1})

    activity_ids = [activity["_id"] for activity in activities]
    if not activity_ids:
        pprint({"Total distance (km)": total_distance})
        return

    # Retrieve all trackpoints for these activities in a single query, including the activity_id
    trackpoints = db.TrackPoint.find({"activity_id": {"$in": activity_ids}},
                                     {"lat": 1, "lon": 1, "date_time": 1, "activity_id": 1}).sort([("activity_id", 1), ("date_time", 1)])

    previous_point = None
    current_activity_id = None

    for tp in trackpoints:
        if tp["activity_id"] != current_activity_id:
            # Reset for a new activity
            previous_point = None
            current_activity_id = tp["activity_id"]

        if previous_point is not None:
            point1 = (previous_point["lat"], previous_point["lon"])
            point2 = (tp["lat"], tp["lon"])
            total_distance += haversine(point1, point2, unit=Unit.KILOMETERS)

        previous_point = tp

    pprint({"Total distance (km)": total_distance})



def task8():
    user_altitude_gain = {}

    # Step 1: Fetch all activities with their user_ids in a single query
    activities = list(db.Activity.find({}, {"_id": 1, "user_id": 1}))

    # Create a dictionary to map activity IDs to user IDs for quick lookup
    activity_user_map = {activity["_id"]: activity["user_id"] for activity in activities}

    # Step 2: Fetch all trackpoints sorted by activity_id and date_time
    trackpoints = db.TrackPoint.find({}, {"activity_id": 1, "altitude": 1, "date_time": 1}).sort([("activity_id", 1), ("date_time", 1)])

    # Initialize tracking variables
    current_activity_id = None
    previous_altitude = None
    total_gain = 0
    user_id = None

    # Process trackpoints
    for tp in trackpoints:
        activity_id = tp["activity_id"]
        altitude = tp["altitude"]

        # Filter altitudes to ensure they are within the valid range
        if altitude < -1292 or altitude > 45000:
            continue

        # If the activity changes, record the total gain for the previous activity's user
        if activity_id != current_activity_id:
            if current_activity_id is not None and total_gain > 0 and user_id is not None:
                user_altitude_gain[user_id] = user_altitude_gain.get(user_id, 0) + total_gain

            # Update variables for the new activity
            current_activity_id = activity_id
            previous_altitude = altitude
            total_gain = 0
            user_id = activity_user_map.get(activity_id)

        else:
            # Calculate altitude gain if the current altitude is higher than the previous one
            if previous_altitude is not None and altitude > previous_altitude:
                altitude_diff = altitude - previous_altitude
                # Only include altitude gain if the difference is less than 3000
                if altitude_diff < 3000:
                    total_gain += altitude_diff
            
            previous_altitude = altitude

    # Account for the last activity processed
    if current_activity_id is not None and total_gain > 0 and user_id is not None:
        user_altitude_gain[user_id] = user_altitude_gain.get(user_id, 0) + total_gain

    # Sort and get the top 20 users
    top_20_users = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]
    pprint([{"user_id": user_id, "total_altitude_gain_meters": str(int(gain)*0.3048,0)} for user_id, gain in top_20_users])

# Find users who have invalid activities
def task9():
    pipeline = [
        {
            "$lookup": {
                "from": "TrackPoint",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "trackpoints"
            }
        },
        {
            "$project": {
                "user_id": 1,
                "trackpoints": {
                    "$filter": {
                        "input": "$trackpoints",
                        "as": "tp",
                        "cond": {
                            "$ne": ["$$tp.date_time", None]
                        }
                    }
                }
            }
        },
        {
            "$unwind": "$trackpoints"
        },
        {
            "$sort": {
                "trackpoints.activity_id": 1,
                "trackpoints.date_time": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "user_id": "$user_id",
                    "activity_id": "$_id"
                },
                "trackpoints": {
                    "$push": "$trackpoints.date_time"
                }
            }
        },
        {
            "$project": {
                "user_id": "$_id.user_id",
                "activity_id": "$_id.activity_id",
                "invalid": {
                    "$gt": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": {
                                        "$map": {
                                            "input": {"$range": [1, {"$size": "$trackpoints"}]},
                                            "as": "idx",
                                            "in": {
                                                "$divide": [
                                                    {
                                                        "$subtract": [
                                                            {"$arrayElemAt": ["$trackpoints", "$$idx"]},
                                                            {"$arrayElemAt": ["$trackpoints", {"$subtract": ["$$idx", 1]}]}
                                                        ]
                                                    },
                                                    60000  # Convert milliseconds to minutes
                                                ]
                                            }
                                        }
                                    },
                                    "as": "time_diff",
                                    "cond": {"$gte": ["$$time_diff", 5]}
                                }
                            }
                        },
                        0
                    ]
                }
            }
        },
        {
            "$match": {
                "invalid": True
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "invalid_activities_count": {"$sum": 1}
            }
        }
    ]

    result = list(db.Activity.aggregate(pipeline, allowDiskUse=True))
    pprint(result)



# Task 10: Find users who have tracked an activity in the Forbidden City
def task10():
    forbidden_lat = 39.916
    forbidden_lon = 116.397
    tolerance = 0.001
    users_in_forbidden_city = set()

    activities = db.Activity.find({}, {"_id": 1, "user_id": 1})
    for activity in activities:
        trackpoints = db.TrackPoint.find(
            {"activity_id": activity["_id"], 
             "lat": {"$gte": forbidden_lat - tolerance, "$lte": forbidden_lat + tolerance},
             "lon": {"$gte": forbidden_lon - tolerance, "$lte": forbidden_lon + tolerance}}
        )
        if trackpoints.count() > 0:
            users_in_forbidden_city.add(activity["user_id"])

    pprint(list(users_in_forbidden_city))


# Task 11: Find all users with registered transportation_mode and their most used transportation_mode
def task11():
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {
            "_id": {"user_id": "$user_id", "mode": "$transportation_mode"},
            "mode_count": {"$sum": 1}
        }},
        {"$sort": {"_id.user_id": 1, "mode_count": -1}},
        {"$group": {
            "_id": "$_id.user_id",
            "most_used_mode": {"$first": "$_id.mode"}
        }},
        {"$sort": {"_id": 1}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(result)


def main():
    # print("Task 1:")
    # task1()
    # print("\nTask 2:")
    # task2()
    # print("\nTask 3:")
    # task3()
    # print("\nTask 4:")
    # task4()
    # print("\nTask 5:")
    # task5()
    # print("\nTask 6a:")
    # task6a()
    # print("\nTask 6b:")
    # task6b()
    # print("\nTask 7:")
    # task7()
    # print("Task 8:")
    # task8()
    print("\nTask 9:")
    task9()
    print("\nTask 10:")
    task10()
    print("\nTask 11:")
    task11()

if __name__ == "__main__":
    main()