import string
from tabnanny import verbose

import nltk
import pandas as pd
nltk.download("stopwords")
from collections import Counter

from nltk.corpus import stopwords
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing.text import Tokenizer
from keras import layers
from tensorflow import keras


#Lectura del dataset.
df = pd.read_csv("./dataset/dataset.csv")
# print("shape of news train dataset:",df.shape)
# print(df.head())
train_size = int(df.shape[0]*0.8)

#stopwords
mystopwords = set(stopwords.words("english"))

def print_statics():
  global df
  print("\nBASIC STATIS:")
  print("- Positive News: ",(df.target == 1).sum())
  print("- Negative News: ",(df.target == -1).sum())
  print("-  Neutral News: ",(df.target == 0).sum())

def remove_punct(text): #Eliminamos signos de puntuaciones de los textos.
  translator = str.maketrans("","",string.punctuation)
  return text.translate(translator)

def remove_stopwords(text):
  global mystopwords
  filtred_word = [word.lower() for word in text.split() if word.lower() not in mystopwords]
  return " ".join(filtred_word)

def counter_words(text_col):
  count = Counter()
  for text in text_col.values:
    for word in text.split():
      count[word]+=1
  
  return count

def print_most_counter(counter):
  print("\nDIFERENT WORDS:",len(counter))
  print("5 MOST COMMON WORDS:  ")
  for section in counter.most_common(5):
    print("  - "+str(section[0])+" : "+str(section[1]))
  print("\n")


  
#TEXT PROCCESING
df["news"] = df.news.map(remove_punct)
df["news"] = df.news.map(remove_stopwords)

#TEXT SEQUENCE
counter_news = counter_words(df.news)
print_most_counter(counter_news)
cantidad_news = len(counter_news)

#SPLIT DATASET IN TRAIN AND VALIDATE
train_df = df[:train_size]
val_df = df[train_size:]
# print("shape of train:",train_df.shape)
# print("shape of validation: ",val_df.shape)

#CREACION DE CONJUNTOS DE ENTRENAMIENTO
train_news = train_df.news.to_numpy()
train_target = train_df.target.to_numpy()
validation_news = val_df.news.to_numpy()
validation_target = val_df.target.to_numpy()
# print("train news shape:",train_news.shape[0])
# print("validation news shape:",validation_news.shape[0])


#TOKENIZACION DE LOS DATOS.
# Vectorize a text corpus by turning each text into a sequence of integers
# Necesitamos el numero de palabras distintas (unique words)
tokenizer = Tokenizer(num_words=cantidad_news)
tokenizer.fit_on_texts(train_news)
word_index = tokenizer.word_index #genera un diccionario de palabras unico con un indice correpondiente a la popularidad de la palabra. populalridad: 1 > 10
#cuando 'fiteamos' un texto, obtenemos el word


#Ahora podemos generar las sequencias de textos,
# (cambiando palabras por sus correspondientes indices),
# (lo hace con el word index, cosa que tiene dentro ya el objeto)
news_train_sequences = tokenizer.texts_to_sequences(train_news)
news_validation_sequences = tokenizer.texts_to_sequences(validation_news)
# print(train_news[0])
# print(news_train_sequences[0])

#Una vez tenemos los textos tokenizados con su correspondiente indice, debemos realizarle un padding. Para que todos los arrays tengan la misma longitud
max_length = 20
train_news_padded = pad_sequences(news_train_sequences, maxlen=max_length, padding="post", truncating="post")
val_news_padded = pad_sequences(news_validation_sequences, maxlen=max_length, padding="post", truncating="post")
# print(train_news[0])
# print(news_train_sequences[0])
# print(train_news_padded[0])

# reverse_word_index = dict([(indice,word)  for indice,word in word_index.items()  ])
# print(reverse_word_index)


#CREACION DEL MODELO:
model = keras.models.Sequential()
model.add(layers.Embedding(cantidad_news,32,input_length = max_length))
model.add(layers.LSTM(64,dropout=0.1))
model.add(layers.Dense(1,activation="sigmoid"))
model.summary()


#CREACION DEL COMPILE
loss = keras.losses.BinaryCrossentropy(from_logits=False)
optim = keras.optimizers.Adam(lr=0.001)
metrics = ["accuracy"]

model.compile(loss= loss,optimizer = optim,metrics = metrics)

#ENTRENAMIENTO DEL MODELO:
model.fit(train_news_padded,train_target,epochs = 20, validation_data=(val_news_padded,validation_target),verbose = 2)


#PREDICIONES DE PRUEBA:
predictions = model.predict(val_news_padded)
print(predictions)
print(validation_target)