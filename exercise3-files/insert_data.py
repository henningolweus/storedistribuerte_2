import os
from datetime import datetime, timedelta
from DbConnector import DbConnector

def create_collections(db):
    # Create collections if they do not exist
    user_collection = db["User"]
    activity_collection = db["Activity"]
    trackpoint_collection = db["TrackPoint"]
    print("Collections created or retrieved successfully.")
    return user_collection, activity_collection, trackpoint_collection

def parse_trackpoint_data(file_path):
    trackpoints = []
    with open(file_path, 'r') as file:
        lines = file.readlines()[6:]  # Skip the first 6 header lines
        for line in lines:
            data = line.strip().split(',')
            latitude = float(data[0])
            longitude = float(data[1])
            altitude = int(round(float(data[3])))
            
            # Validate latitude, longitude, and altitude
            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180) or altitude == -777:
                continue
            
            # Convert the date and time to the required format (YYYY-MM-DD HH:MM:SS)
            date_str = f"{data[5]} {data[6]}"
            date_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            
            trackpoint = {
                "lat": latitude,
                "lon": longitude,
                "altitude": altitude,
                "date_days": float(data[4]),
                "date_time": date_time  # Store as a datetime object
            }
            trackpoints.append(trackpoint)
    return trackpoints

def match_transportation_mode(user_id, start_time, end_time):
    # Read labels.txt file for each labeled user and match exact start and end time
    labels_file = f"../dataset/dataset/Data/{user_id}/labels.txt"  # Update this path as needed
    if not os.path.exists(labels_file):
        return None
    
    with open(labels_file, 'r') as file:
        lines = file.readlines()[1:]  # Skip the header
        for line in lines:
            data = line.strip().split('\t')
            label_start = datetime.strptime(data[0], "%Y/%m/%d %H:%M:%S")
            label_end = datetime.strptime(data[1], "%Y/%m/%d %H:%M:%S")
            mode = data[2]
            
            # Ensure the dates are in the format YYYY-MM-DD HH:MM:SS
            label_start = label_start.strftime("%Y-%m-%d %H:%M:%S")
            label_end = label_end.strftime("%Y-%m-%d %H:%M:%S")
            
            # Convert back to datetime objects for comparison
            label_start = datetime.strptime(label_start, "%Y-%m-%d %H:%M:%S")
            label_end = datetime.strptime(label_end, "%Y-%m-%d %H:%M:%S")
            
            if label_start == start_time and label_end == end_time:
                return mode
    return None

def insert_data(db, dataset_path, labeled_users, continue_from_user, continue_from_activity):
    user_collection, activity_collection, trackpoint_collection = create_collections(db)
    
    # Iterate through the users in the dataset directory
    total_users = len(os.listdir(dataset_path))
    for user_index, user_id in enumerate(os.listdir(dataset_path), start=1):
        if int(user_id) < int(continue_from_user):
            print(f"Continuing from memory, skipping user: {user_id}")
            continue

        user_dir = os.path.join(dataset_path, user_id, 'Trajectory')
        if not os.path.isdir(user_dir):
            continue
        
        print(f"Processing user {user_index}/{total_users}: {user_id}")

        if user_collection.find_one({"_id": user_id}):
            print(f"User {user_id} already exists. Skipping insertion.")
        else:
            # Insert user into the User collection
            user_doc = {
                "_id": user_id,
                "has_labels": user_id in labeled_users  # labeled_users is a set of IDs with labels
            }
            user_collection.insert_one(user_doc)
        
        # Iterate through each activity file for the user
        activity_files = os.listdir(user_dir)
        total_activities = len(activity_files)
        for activity_index, file_name in enumerate(activity_files, start=1):
            if int(user_id) == int(continue_from_user) and int(continue_from_activity)>int(activity_index):
                print(f"Continuing from memory, skipping activity:{activity_index}")
                continue
            file_path = os.path.join(user_dir, file_name)
            trackpoints = parse_trackpoint_data(file_path)
            
            # Only insert activities with 2500 or fewer trackpoints
            if len(trackpoints) > 2500:
                continue
            
            # Check if trackpoints is not empty
            if not trackpoints:
                print(f"Skipping activity {activity_index} for user {user_id}: No valid trackpoints found.")
                continue

            print(f"  Processing activity {activity_index}/{total_activities} for user {user_id}")

            # Match transportation mode if available
            start_time = trackpoints[0]['date_time']
            end_time = trackpoints[-1]['date_time']
            
            # Convert the start and end times to the required format
            start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
            
            transportation_mode = match_transportation_mode(user_id, start_time, end_time)
            
            # Insert the activity
            activity_doc = {
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_date_time": start_time,
                "end_date_time": end_time
            }
            activity_id = activity_collection.insert_one(activity_doc).inserted_id
            
            # Insert the trackpoints and link them to the activity
            for tp in trackpoints:
                tp["activity_id"] = activity_id
            trackpoint_collection.insert_many(trackpoints)

        print(f"Finished processing user {user_id}.\n")

def main():

    continue_from_user = "011"
    continue_from_activity = 1 #1-indexed
    db = DbConnector().db
    dataset_path = "../dataset/dataset/Data"  # Update this path
    labeled_users_path = "../dataset/dataset/labeled_ids.txt"  # Update this path

    # Read labeled user IDs
    with open(labeled_users_path, 'r') as f:
        labeled_users = {line.strip() for line in f}

    insert_data(db, dataset_path, labeled_users, continue_from_user, continue_from_activity)

if __name__ == "__main__":
    main()
