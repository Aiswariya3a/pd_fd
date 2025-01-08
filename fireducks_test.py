import fireducks.pandas as pd  # Use FireDucks pandas
import time
import json

def save_metrics_to_json(metrics_data, session_type="fireducks"):
    """Save the processing times to a JSON file, with separate session counters for FireDucks and Pandas."""
    filename = "static/metrics_history.json"
    
    try:
        with open(filename, "r") as f:
            data = json.load(f)  # Load the existing metrics data
            print("Loaded existing data:", data)
    except FileNotFoundError:
        print(f"File {filename} not found. Creating a new file.")
        data = {
            "fireducks": {},
            "pandas": {}
        }

    if "fireducks" not in data:
        data["fireducks"] = {}
    

    # Determine the session counter for the respective type
    if session_type == "fireducks":
        session_id = len(data["fireducks"]) + 1
        session_key = f"session_{session_id}"
        data["fireducks"][session_key] = {"processing_time": metrics_data}
    elif session_type == "pandas":
        session_id = len(data["pandas"]) + 1
        session_key = f"session_{session_id}"
        data["pandas"][session_key] = {"processing_time": metrics_data}
    else:
        print(f"Unknown session type: {session_type}")
        return
    
    # Save the updated data
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
            print(f"Metrics data saved successfully to {filename}.")
    except Exception as e:
        print(f"Failed to write to {filename}: {e}")



    # Save the updated data
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
            print(f"Metrics data saved successfully to {filename}.")
    except Exception as e:
        print(f"Failed to write to {filename}: {e}")


def find_common_viewpoint(data):
    """Calculate median head pose for each zone."""
    zones = ["left", "center", "right"]
    zone_median_pose = {}

    for zone in zones:
        zone_data = data[data['zone'] == zone]
        if not zone_data.empty:
            median_pitch = zone_data['pose.pitch'].median()
            median_yaw = zone_data['pose.yaw'].median()
            median_roll = zone_data['pose.roll'].median()
            zone_median_pose[zone] = {
                "median_pitch": median_pitch,
                "median_yaw": median_yaw,
                "median_roll": median_roll
            }

    return zone_median_pose


def calculate_engagement(chunk, zone_median_pose):
    """Calculate engagement scores for a chunk."""
    emotion_weights = {
        "neutral": 20,
        "happy": -5,
        "sad": 20,
        "angry": 5,
        "surprise": -10,
        "fear": -5,
        "disgust": -30,
        "NaN": -100
    }

    # Map emotion weights
    chunk["emotion_weight"] = chunk["emotion"].map(emotion_weights).fillna(0)
    chunk["weighted_emotion"] = chunk["emotion_weight"] * chunk["confidence"]

    # Retrieve the median pose values for each zone
    zone_median_df = pd.DataFrame(zone_median_pose).T.reset_index().rename(columns={
        "index": "zone",
        "median_pitch": "zone_median_pitch",
        "median_yaw": "zone_median_yaw",
        "median_roll": "zone_median_roll"
    })
    chunk = chunk.merge(zone_median_df, on="zone", how="left")

    # Calculate deviations
    chunk["pitch_deviation"] = (chunk["pose.pitch"] - chunk["zone_median_pitch"]).abs().clip(upper=100)
    chunk["yaw_deviation"] = (chunk["pose.yaw"] - chunk["zone_median_yaw"]).abs().clip(upper=100)

    # Normalize scores
    max_deviation = 45
    chunk["yaw_score"] = (100 - (chunk["yaw_deviation"] / max_deviation * 100)).clip(lower=0)
    chunk["pitch_score"] = (100 - (chunk["pitch_deviation"] / max_deviation * 100)).clip(lower=0)

    # Aggregate scores
    chunk["head_pose_score"] = (chunk["yaw_score"] * 0.7) + (chunk["pitch_score"] * 0.3)
    chunk["normalized_emotion"] = ((chunk["weighted_emotion"] + 50).clip(lower=0, upper=100))
    chunk["engagement_score"] = ((chunk["head_pose_score"] * 0.8) + (chunk["normalized_emotion"] * 0.2)).clip(lower=0, upper=100)

    return chunk[["engagement_score", "zone", "region", "college_name"]]


def generate_engagement_report(csv_file):
    """Process the entire dataset and calculate engagement scores."""
    start_time = time.time()
    print("=== Starting Engagement Report Generation ===\n")

    # Step 1: Reading the entire CSV file
    print("Reading the entire CSV file...")
    data = pd.read_csv(csv_file)

    # Step 2: Optimizing data types
    data['zone'] = data['zone'].astype('category')
    data['emotion'] = data['emotion'].astype('category')

    # Step 3: Finding common viewpoints for the dataset
    print("Calculating common viewpoints...")
    zone_median_pose = find_common_viewpoint(data)

    # Step 4: Calculating engagement scores for the entire dataset
    print("Calculating engagement scores...")
    processed_data = calculate_engagement(data, zone_median_pose)

    # Step 5: Calculating final metrics
    print("Calculating final metrics...")
    final_region_scores = processed_data.groupby('region')['engagement_score'].mean().sort_values(ascending=False)
    final_institution_scores = processed_data.groupby('college_name')['engagement_score'].mean().sort_values(ascending=False)
    final_overall_score = processed_data['engagement_score'].mean()

    total_processing_time = time.time() - start_time
    print(f"Completed processing in {total_processing_time:.2f} seconds.\n")
    save_metrics_to_json(total_processing_time)

    return final_region_scores, final_institution_scores, final_overall_score, total_processing_time


def main():
    csv_file = "large_dataset_new.csv"

    print("=== Starting Engagement Report Workflow ===")
    total_start = time.time()

    region_scores, institution_scores, overall_score, total_chunk_time = generate_engagement_report(csv_file)

    save_metrics_to_json(total_chunk_time)

    total_end = time.time()
    print(f"=== Workflow Completed in {total_end - total_start:.2f} seconds ===")
    return region_scores, institution_scores, overall_score, total_chunk_time
