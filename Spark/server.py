from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Inicializacion de Contexto
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR") ##-> Ocultar warnings 
ssc = StreamingContext(sc,3) #lectura cada 3s
#Procesamiento de los datos

lines = ssc.socketTextStream("localhost",12345)

#===== Procesamiento de los datos =======

lines.flatMap(lambda x: x.split("\n")).map(lambda x: x.split(";")).pprint()

#========================================

#Fin del Bucle
ssc.start()
ssc.awaitTermination()
