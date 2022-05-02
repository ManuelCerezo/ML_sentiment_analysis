from flask import Flask, request,render_template
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app, resources={r"/puerta-enlace/*":{"origins":"*"}})

vader_analyzer_data = []
process_time_data = []

@app.route('/puerta-enlace/setdatos',methods=['POST'])
def set_data():
    global vader_analyzer_data
    global process_time_data

    vader_analyzer_data = request.json["vader_polarity"]
    process_time_data = request.json["process_time"]

    return {"status":"ok"}

@app.route('/puerta-enlace/getdatos',methods=['GET'])
def get_data():
    global vader_analyzer_data
    global process_time_data
    response_data = {}
    response_data['vader_analyzer_data'] = vader_analyzer_data
    response_data['process_time_data'] = process_time_data
    return response_data

if __name__ == '__main__':
    app.run(debug=True,port=5000)