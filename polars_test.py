import polars as pl
import time
import json


def save_metrics_to_json(metrics_data, session_type="pandas"):
    """Save the processing times to a JSON file, with separate session counters for FireDucks, Pandas, and Polars."""
    filename = "static/metrics_history.json"
    
    try:
        with open(filename, "r") as f:
            data = json.load(f)  # Load the existing metrics data
            print("Loaded existing data:", data)
    except FileNotFoundError:
        print(f"File {filename} not found. Creating a new file.")
        data = {
            "fireducks": {},
            "pandas": {},
            "polars": {}
        }

    # Ensure the key exists even if file was loaded from disk
    if session_type not in data:
        data[session_type] = {}

    # Determine the session counter for the respective type
    if session_type == "fireducks":
        session_id = len(data["fireducks"]) + 1
        session_key = f"session_{session_id}"
        data["fireducks"][session_key] = {"processing_time": metrics_data}
    elif session_type == "pandas":
        session_id = len(data["pandas"]) + 1
        session_key = f"session_{session_id}"
        data["pandas"][session_key] = {"processing_time": metrics_data}
    elif session_type == "polars":
        session_id = len(data["polars"]) + 1
        session_key = f"session_{session_id}"
        data["polars"][session_key] = {"processing_time": metrics_data}
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



def find_common_viewpoint(data: pl.DataFrame):
    """Calculate median head pose for each zone using Polars."""
    zones = ["left", "center", "right"]
    zone_median_pose = {}

    for zone in zones:
        zone_data = data.filter(pl.col("zone") == zone)
        if zone_data.height > 0:
            median_pitch = zone_data.select(pl.col("pose.pitch").median()).item()
            median_yaw = zone_data.select(pl.col("pose.yaw").median()).item()
            median_roll = zone_data.select(pl.col("pose.roll").median()).item()
            zone_median_pose[zone] = {
                "median_pitch": median_pitch,
                "median_yaw": median_yaw,
                "median_roll": median_roll
            }

    return zone_median_pose


def calculate_engagement(chunk: pl.DataFrame, zone_median_pose: dict):
    """Calculate engagement scores for a chunk using Polars."""
    
    # Map emotion strings to weights using a when-then-otherwise chain
    chunk = chunk.with_columns([
        pl.when(pl.col("emotion").is_null()).then(0)
          .when(pl.col("emotion") == "neutral").then(20)
          .when(pl.col("emotion") == "happy").then(-5)
          .when(pl.col("emotion") == "sad").then(20)
          .when(pl.col("emotion") == "angry").then(5)
          .when(pl.col("emotion") == "surprise").then(-10)
          .when(pl.col("emotion") == "fear").then(-5)
          .when(pl.col("emotion") == "disgust").then(-30)
          .when(pl.col("emotion") == "NaN").then(-100)
          .otherwise(0)
          .alias("emotion_weight")
    ])

    # Create weighted emotion column
    chunk = chunk.with_columns([
        (pl.col("emotion_weight") * pl.col("confidence")).alias("weighted_emotion")
    ])

    # Create a DataFrame from zone_median_pose and cast "zone" to categorical to match the left DataFrame
    zones = list(zone_median_pose.keys())
    zone_median_df = pl.DataFrame({
        "zone": zones,
        "zone_median_pitch": [zone_median_pose[z]["median_pitch"] for z in zones],
        "zone_median_yaw": [zone_median_pose[z]["median_yaw"] for z in zones],
        "zone_median_roll": [zone_median_pose[z]["median_roll"] for z in zones]
    }).with_columns([
        pl.col("zone").cast(pl.Categorical)
    ])

    # Merge the median pose values (this may emit a warning about remapping; you can enable a global string cache to mitigate it)
    chunk = chunk.join(zone_median_df, on="zone", how="left")

    # Calculate deviations; use clip(0, 100) to ensure values are between 0 and 100
    chunk = chunk.with_columns([
        ((pl.col("pose.pitch") - pl.col("zone_median_pitch")).abs().clip(0, 100)).alias("pitch_deviation"),
        ((pl.col("pose.yaw") - pl.col("zone_median_yaw")).abs().clip(0, 100)).alias("yaw_deviation")
    ])

    max_deviation = 45
    # For yaw_score and pitch_score, clip the values between 0 and 100
    chunk = chunk.with_columns([
        (100 - (pl.col("yaw_deviation") / max_deviation * 100)).clip(0, 100).alias("yaw_score"),
        (100 - (pl.col("pitch_deviation") / max_deviation * 100)).clip(0, 100).alias("pitch_score")
    ])

    # Aggregate scores, clipping results between 0 and 100
    chunk = chunk.with_columns([
        ((pl.col("yaw_score") * 0.7) + (pl.col("pitch_score") * 0.3)).alias("head_pose_score"),
        ((pl.col("weighted_emotion") + 50).clip(0, 100)).alias("normalized_emotion"),
    ])
    chunk = chunk.with_columns([
        ((pl.col("head_pose_score") * 0.8) + (pl.col("normalized_emotion") * 0.2))
        .clip(0, 100)
        .alias("engagement_score")
    ])

    # Select the relevant columns
    return chunk.select(["engagement_score", "zone", "region", "college_name"])




def generate_engagement_report(csv_file):
    """Process the entire dataset and calculate engagement scores using Polars."""
    start_time = time.time()
    print("=== Starting Engagement Report Generation ===\n")

    # Step 1: Reading the entire CSV file
    print("Reading the entire CSV file...")
    data = pl.read_csv(csv_file)

    # Step 2: Optimizing data types
    data = data.with_columns([
        pl.col("zone").cast(pl.Categorical),
        pl.col("emotion").cast(pl.Categorical)
    ])

    # Step 3: Finding common viewpoints for the dataset
    print("Calculating common viewpoints...")
    zone_median_pose = find_common_viewpoint(data)

    # Step 4: Calculating engagement scores for the entire dataset
    print("Calculating engagement scores...")
    processed_data = calculate_engagement(data, zone_median_pose)

    # Step 5: Calculating final metrics
    print("Calculating final metrics...")
    final_region_scores = (
        processed_data
        .group_by("region")
        .agg(pl.col("engagement_score").mean().alias("mean_engagement"))
        .sort("mean_engagement", descending=True)
    )
    final_institution_scores = (
        processed_data
        .group_by("college_name")
        .agg(pl.col("engagement_score").mean().alias("mean_engagement"))
        .sort("mean_engagement", descending=True)
    )
    overall_score = processed_data.select(pl.col("engagement_score").mean().alias("overall_engagement")).item()

    total_processing_time = time.time() - start_time
    print(f"Completed processing in {total_processing_time:.2f} seconds.\n")
    save_metrics_to_json(total_processing_time, session_type="polars")

    return final_region_scores, final_institution_scores, overall_score, total_processing_time



def main():
    csv_file = "large_dataset_new.csv"

    print("=== Starting Engagement Report Workflow ===")
    total_start = time.time()

    region_scores, institution_scores, overall_score, total_chunk_time = generate_engagement_report(csv_file)

    save_metrics_to_json(total_chunk_time, session_type="polars")

    total_end = time.time()
    print(f"=== Workflow Completed in {total_end - total_start:.2f} seconds ===")
    return region_scores, institution_scores, overall_score, total_chunk_time


if __name__ == "__main__":
    main()
