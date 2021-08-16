from flask import Flask, render_template, request
import os
import pandas as pd
import csv


app = Flask(__name__)
# homepage
@app.route('/', methods=['GET', 'POST'])
def home():
    return render_template('index.html') 

# data display
@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == 'POST':
        # read CSV
        file_data = []
        file_data = request.files['csvfile']
        
        if file_data:
            csv_data = pd.read_csv(file_data, encoding='utf8')
 
            # type of columns
            print(csv_data.dtypes)
            # summarize the central tegitndency, dispersion 
            # and shape of a dataset’s distribution
            # print(csv_data.describe())
            
    return render_template('data.html', data = csv_data.dtypes) 

if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host='0.0.0.0', port=8080)
