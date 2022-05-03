from django.shortcuts import render
from flask import Flask, request,render_template
from flask_cors import CORS

app = Flask(__name__,template_folder="templates")
cors = CORS(app, resources={r"/puerta-enlace/*":{"origins":"*"}})
numero = 0
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


@app.route('/prueba-real',methods=['GET'])
def plotter2(): 
    return render_template("grafico2.html")
    

@app.route('/puerta-enlace/get-valor',methods=['GET'])
def getvalor():
    global numero
    numero = numero + 1
    return {"numero":numero}


@app.route('/plot',methods=['GET'])
def plotter():
    data = [
        ("01-01-2020",1597),
        ("02-01-2020",1600),
        ("03-01-2020",1400),
        ("04-01-2020",1250),
        ("05-01-2020",1369),
        ("06-01-2020",1800),
        ("07-01-2020",1960),
        ("08-01-2020",1000)
    ]
    labels = [row[0] for row in data]
    values = [row[1] for row in data]


    return render_template("grafico.html",labels = labels,values = values)

if __name__ == '__main__':
    app.run(debug=True,port=5000)