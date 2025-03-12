from flask import Flask, render_template, jsonify
import fireducks_test  # import the fireducks_test module
import pandas_test    # import the pandas_test module
import polars_test    # import the polars_test module
import json

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/fireducks_report')
def fireducks_report():
    region_scores, institution_scores, overall_score, total_chunk_time = fireducks_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('fireducks_report.html', 
                           region_scores=region_scores, 
                           institution_scores=institution_scores, 
                           overall_score=overall_score, 
                           total_chunk_time=total_chunk_time)

@app.route('/pandas_report')
def pandas_report():
    region_scores, institution_scores, overall_score, total_chunk_time = pandas_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('pandas_report.html', 
                           region_scores=region_scores, 
                           institution_scores=institution_scores, 
                           overall_score=overall_score, 
                           total_chunk_time=total_chunk_time)

@app.route('/polars_report')
def polars_report():
    region_scores_df, institution_scores_df, overall_score, total_chunk_time = polars_test.generate_engagement_report('large_dataset_new.csv')
    # Convert Polars DataFrames to dictionaries for template iteration
    region_scores = {row["region"]: row["mean_engagement"] for row in region_scores_df.to_dicts()}
    institution_scores = {row["college_name"]: row["mean_engagement"] for row in institution_scores_df.to_dicts()}
    return render_template('polars_report.html', 
                           region_scores=region_scores, 
                           institution_scores=institution_scores, 
                           overall_score=overall_score, 
                           total_chunk_time=total_chunk_time)

@app.route('/run_all_tests')
def run_all_tests():
    # Run FireDucks test
    fd_region_scores, fd_institution_scores, fd_overall_score, fd_total_time = fireducks_test.generate_engagement_report('large_dataset_new.csv')
    
    # Run Pandas test
    pd_region_scores, pd_institution_scores, pd_overall_score, pd_total_time = pandas_test.generate_engagement_report('large_dataset_new.csv')
    
    # Run Polars test and convert dataframes to dictionaries
    pol_region_scores_df, pol_institution_scores_df, pol_overall_score, pol_total_time = polars_test.generate_engagement_report('large_dataset_new.csv')
    pol_region_scores = {row["region"]: row["mean_engagement"] for row in pol_region_scores_df.to_dicts()}
    pol_institution_scores = {row["college_name"]: row["mean_engagement"] for row in pol_institution_scores_df.to_dicts()}
    
    return render_template('all_tests_report.html',
                           # FireDucks results
                           fd_region_scores=fd_region_scores,
                           fd_institution_scores=fd_institution_scores,
                           fd_overall_score=fd_overall_score,
                           fd_total_time=fd_total_time,
                           # Pandas results
                           pd_region_scores=pd_region_scores,
                           pd_institution_scores=pd_institution_scores,
                           pd_overall_score=pd_overall_score,
                           pd_total_time=pd_total_time,
                           # Polars results
                           pol_region_scores=pol_region_scores,
                           pol_institution_scores=pol_institution_scores,
                           pol_overall_score=pol_overall_score,
                           pol_total_time=pol_total_time)

@app.route('/get_metrics', methods=['GET'])
def get_metrics():
    """API endpoint to fetch metrics data."""
    filename = "static/metrics_history.json"
    try:
        with open(filename, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        return jsonify({"error": "Metrics file not found"}), 404

    fireducks_data = [
        {"session": key, "processing_time": value["processing_time"]}
        for key, value in data.get("fireducks", {}).items()
    ]
    pandas_data = [
        {"session": key, "processing_time": value["processing_time"]}
        for key, value in data.get("pandas", {}).items()
    ]
    polars_data = [
        {"session": key, "processing_time": value["processing_time"]}
        for key, value in data.get("polars", {}).items()
    ]

    return jsonify({
        "fireducks": fireducks_data, 
        "pandas": pandas_data, 
        "polars": polars_data
    })

@app.route('/metrics_chart')
def metrics_chart():
    """Renders the real-time chart page for metrics."""
    return render_template('metrics_chart.html')

if __name__ == '__main__':
    app.run(debug=True)
