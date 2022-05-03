import findspark
import pandas as pd
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="LOGISTICREG")
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

#Required libraries
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF

from pyspark.ml.classification import RandomForestClassificationModel


rf_model = RandomForestClassificationModel.load("./Model_RF_V1")

# Para que funcione la clase es necesario que se cree primero 
# el modelo contenido en RandomForest.ipnb
class ModelRandForest():

    def __init__(self,data):
        self.data = data
        self.df = spark.createDataFrame([(data,)], ["news"])
        tokenization=Tokenizer(inputCol='news',outputCol='tokens')
        tokenized_df=tokenization.transform(self.df)
        stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
        refined_df=stopword_removal.transform(tokenized_df)
        hashingTF = HashingTF(inputCol="refined_tokens", outputCol="rawFeatures", numFeatures=20)
        featurizedData = hashingTF.transform(refined_df)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        self.rescaledData = idfModel.transform(featurizedData)

    def predict(self):
        self.predictions = rf_model.transform(self.rescaledData)
        probabilidad = self.predictions.head().probability
        if probabilidad[0] > (probabilidad[1] and probabilidad[2]):#negativo
            #print('negativo')
            probabilidad = probabilidad[0]*-1
        elif probabilidad[1] > (probabilidad[0] and probabilidad[2]):#neutro
            #print('neutro')
            probabilidad = 1 - probabilidad[1]
        elif probabilidad[2] >(probabilidad[0] and probabilidad[1]):#positivo
            #print('positivo')
            probabilidad = probabilidad[2]
        
        return probabilidad

# pepe = ModelRandForest('iudewfiuwbfewiu')
# probabilidad = pepe.predict()