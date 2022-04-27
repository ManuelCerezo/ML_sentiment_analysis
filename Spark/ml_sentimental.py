import numpy as np
import pandas as pd
import tensorflow as tf
import string
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
from collections import Counter


#Lectura del dataset.
df = pd.read_csv("./dataset/dataset.csv")
# print("shape of news train dataset:",df.shape)
# print(df.head())

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
  print("\n5 MOST COMMON WORDS: ")
  for section in counter.most_common(5):
    print("  - "+str(section[0])+" : "+str(section[1]))
  print("\n")

if __name__ == '__main__':
  
  df["news"] = df.news.map(remove_punct)
  df["news"] = df.news.map(remove_stopwords)
  words = counter_words(df.news)
  print("diferent words:",len(words))
  print_most_counter(words)
  print(df.head())
  pass