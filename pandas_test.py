import pandas as pd  # Use pandas
import time,json

def save_metrics_to_json(metrics_data):
    """Save the processing times to a JSON file."""
    filename="static/metrics_history.json"
    try:
        with open(filename, "r") as f:
            data = json.load(f)  # Load the existing metrics data
    except FileNotFoundError:
        data = {}

    # Add the new session data
    session_key = f"session_{len(data) + 1}_fireducks"
    data[session_key] = metrics_data

    # Save the updated data
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

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

    # Convert FireDucks DataFrame to pandas DataFrame for iteration
    chunk_pd = chunk
    scores = []

    for _, row in chunk_pd.iterrows():
        emotion = row["emotion"]
        confidence = row["confidence"]
        emotion_weight = emotion_weights.get(emotion, 0) * confidence

        zone_pose = zone_median_pose.get(row["zone"], {"median_pitch": 0, "median_yaw": 0})
        pitch_deviation = abs(row["pose.pitch"] - zone_pose["median_pitch"])
        yaw_deviation = abs(row["pose.yaw"] - zone_pose["median_yaw"])

        # Handle extreme deviations
        yaw_deviation = min(yaw_deviation, 100)
        pitch_deviation = min(pitch_deviation, 100)

        # Normalize scores
        max_deviation = 45
        yaw_score = max(0, 100 - (yaw_deviation / max_deviation) * 100)
        pitch_score = max(0, 100 - (pitch_deviation / max_deviation) * 100)

        # Aggregate scores
        head_pose_score = (yaw_score * 0.7) + (pitch_score * 0.3)
        normalized_emotion = max(min(emotion_weight + 50, 100), 0)  # Clamp value between 0 and 100
        total_engagement_score = max(min((head_pose_score * 0.8) + (normalized_emotion * 0.2), 100), 0)

        scores.append(total_engagement_score)

    # Convert back to FireDucks DataFrame
    result = chunk.copy()
    result['engagement_score'] = scores
    return result

def generate_engagement_report(csv_file):
    """Process the dataset and calculate engagement scores."""
    chunk_size = 50000
    processed_chunks = []
    
    start_time = time.time()
    print("=== Starting Engagement Report Generation ===\n")

    # Step 1: Reading the CSV file in chunks
    t0 = time.time()
    print("Reading CSV file in chunks...")
    
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        # Step 2: Optimizing data types
        chunk['zone'] = chunk['zone'].astype('category')
        chunk['emotion'] = chunk['emotion'].astype('category')

        # Step 3: Finding common viewpoints for the chunk
        t1 = time.time()
        print(f"Processing chunk of size {len(chunk)}...")
        zone_median_pose = find_common_viewpoint(chunk)

        # Step 4: Calculating engagement scores for the chunk
        processed_chunk = calculate_engagement(chunk, zone_median_pose)
        processed_chunks.append(processed_chunk)
        t2 = time.time()
        print(f"Chunk processed in {t2 - t1:.2f} seconds.")
    
    total_chunk_processing_time = time.time() - t0
    print(f"\nCompleted reading and processing chunks in {total_chunk_processing_time:.2f} seconds.\n")
    
    # Step 5: Combining all chunks and calculating final metrics
    print("Combining chunks and calculating final metrics...")
    t3 = time.time()
    combined_data = pd.concat(processed_chunks)
    
    # Calculate the average engagement scores
    final_region_scores = combined_data.groupby('region')['engagement_score'].mean().sort_values(ascending=False)
    final_institution_scores = combined_data.groupby('college_name')['engagement_score'].mean().sort_values(ascending=False)
    final_overall_score = combined_data['engagement_score'].mean()
    
    t4 = time.time()
    print(f"Final metrics calculated in {t4 - t3:.2f} seconds.\n")
    metrics_time = t4 - t3
    
    return final_region_scores, final_institution_scores, final_overall_score, total_chunk_processing_time, metrics_time

def main():
    csv_file = "large_dataset_new.csv"

    print("=== Starting Engagement Report Workflow ===")
    total_start = time.time()
    
    region_scores, institution_scores, overall_score, total_chunk_time, metrics_time = generate_engagement_report(csv_file)

    save_metrics_to_json(total_chunk_time)
    
    # Return region and institution scores as tables (Pandas DataFrames)
    total_end = time.time()
    print(f"=== Workflow Completed in {total_end - total_start:.2f} seconds ===")
    return region_scores, institution_scores, overall_score, total_chunk_time, metrics_time

# Run the complete workflow
def main():
    csv_file = "large_dataset_new.csv"

    print("=== Starting Engagement Report Workflow ===")
    total_start = time.time()
    
    region_scores, institution_scores, overall_score, total_chunk_time, metrics_time = generate_engagement_report(csv_file)
    
    total_end = time.time()
    print(f"=== Workflow Completed in {total_end - total_start:.2f} seconds ===")
    return region_scores, institution_scores, overall_score, total_chunk_time, metrics_time
