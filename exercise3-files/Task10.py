from DbConnector import DbConnector

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

if __name__ == "__main__":
    task10()
