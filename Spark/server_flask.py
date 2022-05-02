from flask import Flask, request,render_template
import requests

app = Flask(__name__)

vader_analyzer_data = []
process_time_data = []

@app.route('/setdatos',methods=['POST'])
def set_data():
    global vader_analyzer_data
    global process_time_data

    vader_analyzer_data = request.json["vader_polarity"]
    process_time_data = request.json["process_time"]

    return {"status":"ok"}

@app.route('/getdatos',methods=['POST'])
def get_data():
    global vader_analyzer_data
    global process_time_data
    response_data = {}
    response_data['vader_analyzer_data'] = vader_analyzer_data
    response_data['process_time_data'] = process_time_data
    return response_data

if __name__ == '__main__':
    app.run(debug=True,port=5000)