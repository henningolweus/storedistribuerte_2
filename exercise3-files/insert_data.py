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
            altitude = int(data[3])
            
            # Validate latitude, longitude, and altitude
            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180) or altitude == -777:
                continue
            
            trackpoint = {
                "lat": latitude,
                "lon": longitude,
                "altitude": altitude,
                "date_days": float(data[4]),
                "date_time": datetime.strptime(f"{data[5]} {data[6]}", "%Y-%m-%d %H:%M:%S")
            }
            trackpoints.append(trackpoint)
    return trackpoints

def is_activity_invalid(trackpoints):
    # An activity is invalid if any consecutive trackpoints have timestamps deviating by 5 minutes or more
    for i in range(1, len(trackpoints)):
        time_diff = trackpoints[i]['date_time'] - trackpoints[i - 1]['date_time']
        if time_diff >= timedelta(minutes=5):
            return True
    return False

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
            
            if label_start == start_time and label_end == end_time:
                return mode
    return None

def insert_data(db, dataset_path):
    user_collection, activity_collection, trackpoint_collection = create_collections(db)
    
    # Iterate through the users in the dataset directory
    for user_id in os.listdir(dataset_path):
        user_dir = os.path.join(dataset_path, user_id, 'Trajectory')
        if not os.path.isdir(user_dir):
            continue
        
        # Insert user into the User collection
        user_doc = {
            "_id": user_id,
            "has_labels": user_id in labeled_users  # labeled_users is a set of IDs with labels
        }
        user_collection.insert_one(user_doc)
        
        # Iterate through each activity file for the user
        for file_name in os.listdir(user_dir):
            file_path = os.path.join(user_dir, file_name)
            trackpoints = parse_trackpoint_data(file_path)
            
            # Only insert activities with 2500 or fewer trackpoints
            if len(trackpoints) > 2500:
                continue
            
            # Check if the activity is invalid based on time deviations
            if is_activity_invalid(trackpoints):
                print(f"Skipping invalid activity in {file_path}")
                continue
            
            # Match transportation mode if available
            start_time = trackpoints[0]['date_time']
            end_time = trackpoints[-1]['date_time']
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

def main():
    db = DbConnector().db
    dataset_path = "/path/to/your/dataset"  # Update this path
    labeled_users_path = "/path/to/labeled_ids.txt"  # Update this path

    # Read labeled user IDs
    with open(labeled_users_path, 'r') as f:
        labeled_users = set(line.strip() for line in f)

    insert_data(db, dataset_path)

if __name__ == "__main__":
    main()
