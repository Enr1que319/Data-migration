from flask import Flask, request
import func
import os

path = '/Users/enriquevazquez/Desktop/Data-migration/Resources'

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    url = request.base_url
    print('Server received request for "Home" page...')
    return (
        f'<h1> HOME (Data Migration) </h1><br>'
    )

@app.route('/api/v1.0/exportData')
def startProcess():
    files_ord = ['departments','jobs','hired_employees']

    try:
        for file in files_ord:
            func.insertData(f'{file}.csv',path)
        return 'Data inserted!'
    except Exception as e:
        print (e, flush = True)
        return None
    
@app.route('/api/v1.0/Metrics')
def getMetrics():

    try:
        a=1+1
        return 'Metrics are done'
    except Exception as e:
        print (e, flush = True)
        return None

if __name__ == "__main__":
    app.run(debug=True)
