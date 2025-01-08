from flask import Flask, render_template, jsonify
import fireducks_test  # import the fireducks_test module
import pandas_test  # import the test_pandas module
import json

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/fireducks_report')
def fireducks_report():
    region_scores, institution_scores, overall_score, total_chunk_time= fireducks_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('fireducks_report.html', region_scores=region_scores, institution_scores=institution_scores, overall_score=overall_score, total_chunk_time=total_chunk_time)

@app.route('/pandas_report')
def pandas_report():
    region_scores, institution_scores, overall_score, total_chunk_time= pandas_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('pandas_report.html', region_scores=region_scores, institution_scores=institution_scores, overall_score=overall_score, total_chunk_time=total_chunk_time)

@app.route('/get_metrics', methods=['GET'])
def get_metrics():
    """API endpoint to fetch metrics data."""
    filename = "static/metrics_history.json"
    try:
        with open(filename, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        return jsonify({"error": "Metrics file not found"}), 404

    # Separate FireDucks and Pandas metrics
    fireducks_data = [
        {"session": key, "processing_time": value["processing_time"]}
        for key, value in data.items() if value["type"] == "fireducks"
    ]
    pandas_data = [
        {"session": key, "processing_time": value["processing_time"]}
        for key, value in data.items() if value["type"] == "pandas"
    ]

    return jsonify({"fireducks": fireducks_data, "pandas": pandas_data})

@app.route('/metrics_chart')
def metrics_chart():
    """Renders the real-time chart page for metrics."""
    return render_template('metrics_chart.html')

if __name__ == '__main__':
    app.run(debug=True)
