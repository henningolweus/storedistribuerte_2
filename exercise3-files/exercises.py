from pymongo import MongoClient
from pprint import pprint
from datetime import timedelta, datetime
from haversine import haversine, Unit
from DbConnector import DbConnector
from collections import defaultdict

#Code has been structured using OpenAI ChatGPT 4.0

# Connect to the database
db_connector = DbConnector()
db = db_connector.db

# Task 1: Count number of users, activities, and trackpoints
def task1():
    num_users = db.User.count_documents({})
    num_activities = db.Activity.count_documents({})
    num_trackpoints = db.TrackPoint.count_documents({})
    print(f"Number of Users: {num_users}")
    print(f"Number of Activities: {num_activities}")
    print(f"Number of Trackpoints: {num_trackpoints}")


# Task 2: Find the average number of activities per user (including those with zero activities)
def task2():
    # Total number of users (including those without activities)
    total_users = db.User.count_documents({})

    # Total number of activities
    total_activities = db.Activity.count_documents({})

    # Calculate the average
    avg_activities = total_activities / total_users if total_users > 0 else 0
    print(f"Average Activities per User: {avg_activities:.2f}")


# Task 3: Find the top 20 users with the highest number of activities
def task3():
    pipeline = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ]
    result = list(db.Activity.aggregate(pipeline))
    print("Top 20 Users by Number of Activities:")
    for i, user in enumerate(result, 1):
        print(f"User ID: {user['_id']}, Activities: {user['activity_count']}")


# Task 4: Find all users who have taken a taxi
def task4():
    pipeline = [
        {"$match": {"transportation_mode": "taxi"}},
        {"$group": {"_id": "$user_id"}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    
    # Extract user IDs, sort them, and join them into a string
    user_ids = sorted([user["_id"] for user in result])
    user_ids_str = ", ".join(user_ids)
    
    # Print the result in the desired format
    print(f"The users who have taken a taxi are {user_ids_str}")

# Task 5: Count transportation modes and activities tagged with them
def task5():
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    result = list(db.Activity.aggregate(pipeline))
    print("Transportation modes and their counts:")
    for item in result:
        print(f"Mode: {item['_id']} | Count: {item['count']}")


# Task 6a: Find the year with the most activities
def task6a():
    pipeline = [
        {"$project": {"year": {"$year": "$start_date_time"}}},
        {"$group": {"_id": "$year", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 1}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(f"Year: {result[0]['_id']} | Activity count: {result[0]['activity_count']}")

# Task 6b: Find the year with the most recorded hours
def task6b():
    pipeline = [
        {"$project": {"year": {"$year": "$start_date_time"}, "duration": {"$subtract": ["$end_date_time", "$start_date_time"]}}},
        {"$group": {"_id": "$year", "total_hours": {"$sum": {"$divide": ["$duration", 3600000]}}}},  # Convert milliseconds to hours
        {"$sort": {"total_hours": -1}},
        {"$limit": 1}
    ]
    result = list(db.Activity.aggregate(pipeline))
    pprint(f"Year: {result[0]['_id']} | Recorded hours: {result[0]['total_hours']}")



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

    pprint(f'Total distance (km): {total_distance}')


# Task 8: Find the top 20 users who have gained the most altitude meters
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
    # Print the top 20 users with properly formatted altitude gain
    pprint([{"user_id": user_id, "total_altitude_gain_meters": f"{gain * 0.3048:.2f}"} for user_id, gain in top_20_users])


# Find users who have invalid activities
def task9():
    # Create an instance of DbConnector to access the database
    db_connector = DbConnector()
    activities_collection = db_connector.db['Activity']  # Accessing the activities collection
    trackpoints_collection = db_connector.db['TrackPoint']  # Accessing the trackpoints collection

    # Get all activities from the database
    activities = list(activities_collection.find())
    
    # Fetch all trackpoints and group them by activity ID
    pipeline = [
        {"$group": {
            "_id": "$activity_id",
            "trackpoints": {"$push": "$date_time"}
        }}
    ]
    
    trackpoints_by_activity = {tp["_id"]: tp["trackpoints"] for tp in trackpoints_collection.aggregate(pipeline)}

    # Initialize a dictionary to store the count of invalid activities per user
    invalid_activity_counts = defaultdict(int)

    for activity in activities:
        # Fetch associated trackpoints for the current activity
        trackpoints = trackpoints_by_activity.get(activity['_id'], [])
        is_invalid = False  # Assume activity is valid initially

        # Sort trackpoints by time if not already sorted
        trackpoints.sort()

        # Check time differences
        for i in range(len(trackpoints) - 1):
            time_diff = (trackpoints[i + 1] - trackpoints[i]).total_seconds()
            
            if time_diff >= 300:  # 5 minutes
                is_invalid = True
                break  # Exit loop as we already found an invalid condition

        # Count invalid activity
        if is_invalid:
            invalid_activity_counts[activity['user_id']] += 1

    # Print the results
    if invalid_activity_counts:
        for user_id, count in invalid_activity_counts.items():
            print(f"User ID: {user_id}, Invalid Activities: {count}")


# Task 10: Find users who have tracked an activity in the Forbidden City
def task10():
    forbidden_city_coords = (39.916, 116.397)
    forbidden_radius_deg = 0.001  # Approximation for 0.1 km tolerance

    # Initialize the database connection
    db_connector = DbConnector()
    db = db_connector.db

    # Ensure an index on TrackPoint coordinates for faster querying
    db.TrackPoint.create_index([("lat", 1), ("lon", 1)])  # Ascending index on lat and lon

    # First filter TrackPoints close to the Forbidden City, then join
    pipeline = [
        {
            "$match": {
                "lat": {"$gte": forbidden_city_coords[0] - forbidden_radius_deg,
                        "$lte": forbidden_city_coords[0] + forbidden_radius_deg},
                "lon": {"$gte": forbidden_city_coords[1] - forbidden_radius_deg,
                        "$lte": forbidden_city_coords[1] + forbidden_radius_deg}
            }
        },
        {
            "$lookup": {
                "from": "Activity",  # Assuming the collection is named "Activity"
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity"
            }
        },
        {
            "$unwind": "$activity"  # Unwind only if activity exists
        },
        {
            "$group": {"_id": "$activity.user_id"}  # Group by unique user IDs
        }
    ]

    # Execute the aggregation pipeline
    result = list(db.TrackPoint.aggregate(pipeline))
    
    # Print the user IDs with activities in the Forbidden City area
    print("Users with Activities in the Forbidden City:")
    user_ids = [str(user["_id"]) for user in result]
    if user_ids:
        print(f"User IDs: {', '.join(user_ids)}")
    else:
        print("No users found with activities in the Forbidden City area.")


# Task 11: Find all users with registered transportation_mode and their most used transportation_mode, handling ties
def task11():
    pipeline = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {
            "_id": {"user_id": "$user_id", "mode": "$transportation_mode"},
            "mode_count": {"$sum": 1}
        }},
        {"$sort": {"_id.user_id": 1}},
        {"$group": {
            "_id": "$_id.user_id",
            "modes": {
                "$push": {
                    "mode": "$_id.mode",
                    "count": "$mode_count"
                }
            },
            "max_count": {"$max": "$mode_count"}
        }},
        {"$project": {
            "_id": 1,
            "most_used_modes": {
                "$filter": {
                    "input": "$modes",
                    "as": "mode",
                    "cond": {"$eq": ["$$mode.count", "$max_count"]}
                }
            }
        }},
        {"$sort": {"_id": 1}}
    ]
    
    result = list(db.Activity.aggregate(pipeline))
    
    # Format the output to display the user IDs with their most used transportation modes, handling ties
    for item in result:
        user_id = item["_id"]
        modes = [mode["mode"] for mode in item["most_used_modes"]]
        modes_str = ", ".join(modes)
        #print(f"User ID: {user_id} | Modes: {modes_str}")
        print(f"The most used transportation mode for user {user_id} was: {modes_str}")


# Function to display counts of activities and trackpoints for a specific user ID
def view_user_summary(user_id):
    # Count activities for the user
    num_activities = db.Activity.count_documents({"user_id": user_id})
    print(f"Number of Activities for User ID {user_id}: {num_activities}")

    # Get all activity IDs for the user
    activity_ids = db.Activity.distinct("_id", {"user_id": user_id})

    # Count trackpoints associated with these activities
    num_trackpoints = db.TrackPoint.count_documents({"activity_id": {"$in": activity_ids}})
    print(f"Number of Trackpoints for User ID {user_id}: {num_trackpoints}")







def main():
    #view_user_summary("068")
    print("\nTask 1:")
    print("-" * 50)
    task1()
    
    print("\nTask 2:")
    print("-" * 50)
    task2()
    
    print("\nTask 3:")
    print("-" * 50)
    task3()
    print("\nTask 4:")
    task4()
    print("\nTask 5:")
    task5()
    print("\nTask 6a:")
    task6a()
    print("\nTask 6b:")
    task6b()
    print("\nTask 7:")
    task7()
    print("Task 8:")
    task8()
    print("\nTask 9:")
    task9()
    print("\nTask 10:")
    task10()
    print("\nTask 11:")
    task11()
    print("\n ----------------------------------------------- \n")


if __name__ == "__main__":
    main()