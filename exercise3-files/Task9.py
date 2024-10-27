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

if __name__ == "__main__":
    task9()
