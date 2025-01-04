from flask import Flask, render_template
import fireducks_test  # import the fireducks_test module
import pandas_test  # import the test_pandas module

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/fireducks_report')
def fireducks_report():
    region_scores, institution_scores, overall_score, total_chunk_time, metrics_time = fireducks_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('fireducks_report.html', region_scores=region_scores, institution_scores=institution_scores, overall_score=overall_score, total_chunk_time=total_chunk_time, metrics_time=metrics_time)

@app.route('/pandas_report')
def pandas_report():
    region_scores, institution_scores, overall_score, total_chunk_time, metrics_time = pandas_test.generate_engagement_report('large_dataset_new.csv')
    return render_template('pandas_report.html', region_scores=region_scores, institution_scores=institution_scores, overall_score=overall_score, total_chunk_time=total_chunk_time, metrics_time=metrics_time)

if __name__ == '__main__':
    app.run(debug=True)
