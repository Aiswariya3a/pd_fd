import time
import pandas as pd
import fireducks.pandas as fd
import numpy as np
from tqdm import tqdm
import os
import gc
from datetime import datetime

def find_common_viewpoint_fireducks(data):
    """Calculate median head pose for each zone using FireDucks."""
    zones = ["left", "center", "right"]
    zone_median_pose = {}

    for zone in zones:
        zone_data = data[data['zone'] == zone]
        if not zone_data.empty:
            median_pitch = zone_data['pose.pitch'].median()
            median_yaw = zone_data['pose.yaw'].median()
            median_roll = zone_data['pose.roll'].median()
            zone_median_pose[zone] = {
                "median_pitch": median_pitch._evaluate(),
                "median_yaw": median_yaw._evaluate(),
                "median_roll": median_roll._evaluate()
            }

    return zone_median_pose

def find_common_viewpoint_pandas(data):
    """Calculate median head pose for each zone using Pandas."""
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

def calculate_engagement_score(emotion):
    """Calculate engagement score based on emotion."""
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
    return emotion_weights.get(emotion, 0)

def process_with_fireducks(csv_file, chunk_size=50000):
    """Process the dataset using FireDucks."""
    t_start = time.time()
    print("\nStarting FireDucks processing...")
    
    chunks = []
    total_rows = 0
    
    # Read and process chunks
    reader = fd.read_csv(csv_file, chunksize=chunk_size)
    for chunk in tqdm(reader, desc="Processing chunks with FireDucks"):
        # Optimize data types and calculate scores in one chain
        processed_chunk = (chunk
            .assign(
                zone=lambda x: x['zone'].astype('category'),
                emotion=lambda x: x['emotion'].astype('category'),
                engagement_score=lambda x: x['emotion'].map(calculate_engagement_score)
            )
        )._evaluate()
        
        chunks.append(processed_chunk)
        total_rows += len(processed_chunk)
    
    t_chunk = time.time()
    chunk_time = t_chunk - t_start
    print(f"Chunk processing completed. Time taken: {chunk_time:.2f} seconds")
    
    # Combine chunks and calculate metrics
    print("Calculating final metrics...")
    combined_data = fd.concat(chunks)._evaluate()
    
    metrics = {
        'region_scores': combined_data.groupby('region')['engagement_score'].mean()._evaluate(),
        'institution_scores': combined_data.groupby('college_name')['engagement_score'].mean()._evaluate(),
        'overall_score': combined_data['engagement_score'].mean()._evaluate(),
        'total_rows': total_rows,
        'chunk_time': chunk_time,
        'total_time': time.time() - t_start
    }
    
    print(f"FireDucks processing completed. Total time: {metrics['total_time']:.2f} seconds")
    return metrics

def process_with_pandas(csv_file, chunk_size=50000):
    """Process the dataset using Pandas."""
    t_start = time.time()
    print("\nStarting Pandas processing...")
    
    chunks = []
    total_rows = 0
    
    # Read and process chunks
    reader = pd.read_csv(csv_file, chunksize=chunk_size)
    for chunk in tqdm(reader, desc="Processing chunks with Pandas"):
        # Process chunk
        chunk['zone'] = chunk['zone'].astype('category')
        chunk['emotion'] = chunk['emotion'].astype('category')
        chunk['engagement_score'] = chunk['emotion'].map(calculate_engagement_score)
        
        chunks.append(chunk)
        total_rows += len(chunk)
    
    t_chunk = time.time()
    chunk_time = t_chunk - t_start
    print(f"Chunk processing completed. Time taken: {chunk_time:.2f} seconds")
    
    # Combine chunks and calculate metrics
    print("Calculating final metrics...")
    combined_data = pd.concat(chunks)
    
    metrics = {
        'region_scores': combined_data.groupby('region')['engagement_score'].mean(),
        'institution_scores': combined_data.groupby('college_name')['engagement_score'].mean(),
        'overall_score': combined_data['engagement_score'].mean(),
        'total_rows': total_rows,
        'chunk_time': chunk_time,
        'total_time': time.time() - t_start
    }
    
    print(f"Pandas processing completed. Total time: {metrics['total_time']:.2f} seconds")
    return metrics

def save_comparison_report(fd_metrics, pd_metrics, output_path=None):
    """Save detailed comparison report."""
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"engagement_comparison_report_{timestamp}.txt"
    
    print(f"\nSaving comparison report to {output_path}")
    
    with open(output_path, "w") as file:
        file.write("=== Engagement Analysis Performance Comparison ===\n")
        file.write(f"Report generated at: {datetime.now()}\n\n")
        
        # Performance metrics
        file.write("=== Performance Metrics ===\n")
        file.write(f"Total rows processed: {fd_metrics['total_rows']}\n\n")
        
        file.write("FireDucks:\n")
        file.write(f"- Chunk processing time: {fd_metrics['chunk_time']:.2f} seconds\n")
        file.write(f"- Total processing time: {fd_metrics['total_time']:.2f} seconds\n\n")
        
        file.write("Pandas:\n")
        file.write(f"- Chunk processing time: {pd_metrics['chunk_time']:.2f} seconds\n")
        file.write(f"- Total processing time: {pd_metrics['total_time']:.2f} seconds\n\n")
        
        # Calculate performance difference
        time_diff = pd_metrics['total_time'] - fd_metrics['total_time']
        speedup = pd_metrics['total_time'] / fd_metrics['total_time']
        file.write(f"Performance difference: {abs(time_diff):.2f} seconds\n")
        file.write(f"Speedup ratio: {speedup:.2f}x\n\n")
        
        # Results comparison
        file.write("=== Results Comparison ===\n")
        file.write("Region-wise Engagement Scores:\n")
        file.write("FireDucks:\n")
        file.write(fd_metrics['region_scores'].to_string())
        file.write("\n\nPandas:\n")
        file.write(pd_metrics['region_scores'].to_string())
        
        file.write("\n\nInstitution-wise Engagement Scores:\n")
        file.write("FireDucks:\n")
        file.write(fd_metrics['institution_scores'].to_string())
        file.write("\n\nPandas:\n")
        file.write(pd_metrics['institution_scores'].to_string())
        
        file.write("\n\nOverall Engagement Scores:\n")
        file.write(f"FireDucks: {fd_metrics['overall_score']:.4f}\n")
        file.write(f"Pandas: {pd_metrics['overall_score']:.4f}\n")
        
        # Verify results match
        file.write("\n=== Results Verification ===\n")
        metrics_match = {
            'region_scores': np.allclose(fd_metrics['region_scores'], pd_metrics['region_scores']),
            'institution_scores': np.allclose(fd_metrics['institution_scores'], pd_metrics['institution_scores']),
            'overall_score': np.allclose(fd_metrics['overall_score'], pd_metrics['overall_score'])
        }
        
        for metric, matches in metrics_match.items():
            file.write(f"{metric} match: {matches}\n")

def main():
    csv_file = "large_dataset_new.csv"
    chunk_size = 50000
    
    print("=== Starting Engagement Analysis Performance Comparison ===")
    print(f"Input file: {csv_file}")
    print(f"Chunk size: {chunk_size}")
    
    # Clear memory before starting
    gc.collect()
    
    # Run FireDucks version
    fd_metrics = process_with_fireducks(csv_file, chunk_size)
    
    # Clear memory between runs
    gc.collect()
    
    # Run Pandas version
    pd_metrics = process_with_pandas(csv_file, chunk_size)
    
    # Save comparison report
    save_comparison_report(fd_metrics, pd_metrics)
    
    print("\nAnalysis completed!")

if __name__ == "__main__":
    main()