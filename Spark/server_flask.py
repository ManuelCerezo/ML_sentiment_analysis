from flask import Flask, request,render_template
from flask_cors import CORS

app = Flask(__name__,template_folder="templates")
cors = CORS(app, resources={r"/puerta-enlace/*":{"origins":"*"}})
numero = 0
data_process = []
labels = []

@app.route('/puerta-enlace/setdatos',methods=['POST'])
def set_data():
    global data_process
    global labels

    data_process = request.json["data_procces"]
    labels = request.json["labels"]

    return {"status":"ok"}

@app.route('/puerta-enlace/getdatos',methods=['GET'])
def get_data():
    global data_process
    global labels
    response_data = {}
    response_data['data_process'] = data_process
    response_data['labels'] = labels
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
    return render_template("grafico.html")

if __name__ == '__main__':
    app.run(debug=True,port=5000)