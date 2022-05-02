import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel





#Inicializacion de Contexto
sc = SparkContext(appName="SERVER")
# sc.setLogLevel("ERROR") ##-> Ocultar warnings 
# ssc = StreamingContext(sc,3) #lectura cada 3s
# vader_analyzer = SentimentIntensityAnalyzer()
#Procesamiento de los datos

# lines = ssc.socketTextStream("localhost",12345)

# #===== Procesamiento de los datos =======
# lines.flatMap(lambda x: x.split("\n")).map(lambda x: x.split(";")).pprint()


model_neg = LogisticRegressionModel.load("../Modelo_ML/model_neg")
model_pos = LogisticRegressionModel.load("../Modelo_ML/model_pos")

print(type(model_neg))

#========================================

# #Fin del Bucle
# ssc.start()
# ssc.awaitTermination()
