from flask import Flask, request,render_template
from flask_cors import CORS

app = Flask(__name__,template_folder="templates")
cors = CORS(app, resources={r"/puerta-enlace/*":{"origins":"*"}})

plot = {'labels':[],'data':{'vader':[0.0],'textblob':[0.0]}}

@app.route('/puerta-enlace/setdatos',methods=['POST'])
def set_data():
    global plot
    data = request.json["data"]
    plot['labels'] = data['labels']
    plot['data']['vader'] = data['vader']
    plot['data']['textblob'] = data['textblob']

    return {"status":"ok"}

@app.route('/puerta-enlace/getdatos',methods=['GET'])
def get_data():
    global plot    
    return plot
    


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