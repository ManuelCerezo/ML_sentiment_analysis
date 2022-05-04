import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from soupsieve import select
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from class_model import



#Inicializacion de Contexto1
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()

#Inicializacion de Contexto2
ssc = StreamingContext(sc,1) #lectura cada 3s
lines = ssc.socketTextStream("localhost",12345)

indice = 0

#Inicializacion de Funciones UDF
apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())
#apply_mymodel = udf(lambda x: ModelRandForest(x).predict(),FloatType())


def data_serialize(rdd):
    global indice  
    df = rdd.toDF(['fuente','url','notice','notice_date','process_date'])
    #df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    #df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    #df = df.withColumn("mymodel_polarity", apply_mymodel(col('notice')))
    df.createOrReplaceTempView("cryptonews"+str(indice))
    #df.show()
    send_to_server()

    

def send_to_server():
    global spark
    global indice
    data_to_sent = {}

    ## obtener datos del dataframe
    ##### SEND TO WEB SERVER ####
    #df = spark.sql('select vader_polarity, textBlob_polarity, process_date from cryptonews')
    df = spark.sql('select * from cryptonews'+str(indice))
    
    #df.show()
    

    try:
        data_to_sent['labels'] = df.select('fuente').rdd.flatMap(lambda x: x).collect()
        print("funciona..")
        #print(data_to_sent['labels'])
    except:
        print("da fallo..")
        print(df.select('process_date').rdd.flatMap(lambda x: x).collect())
        
    # data_to_sent['vader'] = df.select("vader_polarity").toPandas().values.T.tolist()[0]
    # data_to_sent['textblob'] = df.select("textBlob_polarity").toPandas().values.T.tolist()[0]

    #data_to_sent['mymodel'] = df.select("mymodel_polarity").toPandas().values.T.tolist()[0]

    
    # requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
    #     "data": data_to_sent
    # })
    


#===================== Procesamiento de los datos =======================
lines.window(5,2).map(lambda x:x.split('/')).foreachRDD(data_serialize)
#========================================================================
#Fin del Bucle
ssc.start()
ssc.awaitTermination()