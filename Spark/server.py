from cProfile import label
from typing import Text
from pyspark import Row, SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob
from matplotlib import pyplot as plt
import matplotlib.animation as animation




#Inicializacion de Contexto
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR") ##-> Ocultar warnings
ssc = StreamingContext(sc,1) #lectura cada 3s
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()
lines = ssc.socketTextStream("localhost",12348)


def data_serialize(rdd):
    global spark

    #Creacion de dataframe de ventana de trabajo
    rowRdd = rdd.map(lambda x: Row( fuente = x[0],url=x[1],news = x[2],notice_date=x[3],\
        vader_polarity = vader_analyzer.polarity_scores(x[2])['compound']\
            ,textblob_polarity = TextBlob(x[3]).sentiment.polarity,\
                process_time = x[4]))

    df = spark.createDataFrame(rowRdd) #Almacenamos los datos temporalmente
    
    df.registerTempTable("cryptonews")
    # select_date_to_dataframeSQL()
    #localplot()
    prueba()
    

def prueba(): #Obtener datos del dataframe
    df_subset = spark.sql(
                    """
                    select fuente, news, vader_polarity, process_time from cryptonews
                    """
                )
    print(df_subset.show())
    print(df_subset.count())

def localplot():
    df_plot = spark.sql(
        '''
        select process_time, vader_polarity,textblob_polarity from cryptonews
        '''
    )
    df_plot = df_plot.toPandas()
    plt.plot(df_plot['process_time'],df_plot['vader_polarity'],label = 'Vader Polarity')
    plt.legend()
    plt.show()


#===== Procesamiento de los datos =======
datos = lines.window(5,1).map(lambda x:x.split(';'))
#datos.window(30,10).map(lambda x: TextBlob(x[2])).pprint()
datos.foreachRDD(data_serialize)
#========================================

#Fin del Bucle
ssc.start()
ssc.awaitTermination()