from pymongo import MongoClient
from datetime import datetime
from DbConnector import DbConnector
from collections import defaultdict

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
    
    trackpoints_by_activity = defaultdict(list)
    for tp in trackpoints_collection.aggregate(pipeline):
        trackpoints_by_activity[tp["_id"]] = tp["trackpoints"]

    # Initialize a dictionary to store the count of invalid activities per user
    invalid_activity_counts = {}

    # Iterate through each activity
    for activity in activities:
        activity_id = activity["_id"]
        activity_trackpoints = trackpoints_by_activity.get(activity_id, [])
        
        if not activity_trackpoints:
            continue

        # Initialize a flag to check for invalid activity
        is_invalid = False

        # Sort trackpoints by date_time
        activity_trackpoints.sort()  # Sort directly, since they are date_time objects
        
        # Check for invalid activities with consecutive trackpoints
        for i in range(1, len(activity_trackpoints)):
            prev_time = activity_trackpoints[i - 1]
            curr_time = activity_trackpoints[i]
            time_diff = (curr_time - prev_time).total_seconds()
            if time_diff >= 300:  # 5 minutes
                is_invalid = True
                break
        
        # If the activity is invalid, increment the count for the user
        if is_invalid:
            user_id = activity["user_id"]
            invalid_activity_counts[user_id] = invalid_activity_counts.get(user_id, 0) + 1

    # Print the results
    if invalid_activity_counts:
        print("Invalid activities by user:")
        for user_id, count in invalid_activity_counts.items():
            print(f"User ID: {user_id}, Invalid Activities: {count}")
    else:
        print("No invalid activities found.")

if __name__ == "__main__":
    task9()
