#CALLER PYTHON REQUIREMENTS
import datetime
from time import sleep
import time
from bs4 import BeautifulSoup
import requests
import tweepy
import socket
import preprocessor as p
from config_twitter_access import TWITTER_BAREER_TOKEN

import findspark
findspark.init()

#SPARK REQUIRENENTS
from pyspark import SparkContext
sc = SparkContext(appName="CALLER")

#CALLER TWITTER METADATA
# from config_twitter_access import TWITTER_BAREER_TOKEN
client = client = tweepy.Client(TWITTER_BAREER_TOKEN)

#METADATA
CODE_SPLIT ='A9RTp15Z'

#CALLER METADATA
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
BASE_URL = 'https://coinmarketcal.com/en/news?page={}'
GELPH_API_URL = 'https://api.gdeltproject.org/api/v2/doc/doc?query={} sourcelang:eng&maxrecords=250&timespan=1day&format=JSON&sort=datedesc'

#SOCKET METADATA
HOST = "localhost"
PORT = 12346 #puerto: 12.345
protocolo_IPV4 = socket.AF_INET
protocolo_TCP = socket.SOCK_STREAM


cantidad = 0

with socket.socket(protocolo_IPV4,protocolo_TCP) as mysocket: #Creación de un socket
    mysocket.bind((HOST,PORT)) #Ponemos el socket a la escucha en el host y puerto indicado
    mysocket.listen()
    print("Esperando conexion de cliente...")
    conn,addr = mysocket.accept() # se queda esperando para conexiones entrantes, cuando se establece una conexion, se devuelve la conexion y la direccion entrante (socket y direccion del cliente)

def get_cripto_notice():
    global cantidad 
    num_page = 0
    active_while = True
    global conn
    while (active_while):
        try:
            sourceCode = requests.get(url= BASE_URL.format(num_page), headers= HEADERS, timeout=10).text
            sourceCode = BeautifulSoup(sourceCode, 'html.parser')
        except:
            pass
        if not sourceCode.find('h5',class_='card__title mb-0'):
            active_while = False

        for notice , fecha in zip(sourceCode.find_all('h5',class_='card__title mb-0'),sourceCode.find_all('h5',class_='card__date')):
            now = datetime.datetime.now() 
            #Send date with all information:
            #sent_information("cointmarket"+"A9RTp15Z"+BASE_URL.format(num_page)+"A9RTp15Z"+notice.text+"A9RTp15Z"+str(datetime.datetime.strptime(fecha.text, '%d %b %Y').date())+"A9RTp15Z"+str(now.time())+"\n",conn)
            
            #Send date with notice and time processing
            sent_information(notice.text+CODE_SPLIT+str(now.time())+'\n',conn)
            print("(coinmarketcal.com) datos mandados: ",cantidad)
            cantidad = cantidad + 1
    
        num_page = num_page + 1

def get_crypto_tweets():
    a = 0
    global conn
    global cantidad
    #themes =['#cryptonews','#cryptocurrence','#bitcoin','#Ethereum','#CNN','#cryptomarket']

    for a in range(0,30):
        response = client.search_recent_tweets(' #cryptonews OR #bitcoin lang:en',max_results = 10, tweet_fields = ['created_at','lang'])
        for tweet in response.data:
            now = datetime.datetime.now() 
            
            sent_information(p.clean(tweet.text)+CODE_SPLIT+str(now.time())+'\n',conn)
            time.sleep(0.5)
        
            cantidad = cantidad + 1
            print("(Twitter) datos mandados: ",cantidad)



def get_crypto_gdelt():
    global conn
    queries =['(crypto OR cryptocurrencies)']
    global cantidad
    
    for query in queries:
        request = requests.get(url=GELPH_API_URL.format(query)).json()

        for article in request['articles']:
            now = datetime.datetime.now() 
            #print(article['seendate'][0:4]+'-'+article['seendate'][4:6]+"-"+article['seendate'][6:8])
            sent_information(deEmojify(article["title"])+CODE_SPLIT+str(now.time())+'\n',conn)
            time.sleep(0.5)
            cantidad = cantidad + 1
            print("(Gdelt) datos mandados: ",cantidad)

        
    print('cantidad noticias: ',cantidad)

def deEmojify(inputString): #quitar emoji a tweets
    return inputString.encode('ascii', 'ignore').decode('ascii')

def sent_information(text, conection):
    conection.send(text.encode('utf-8'))
    pass

if __name__ == "__main__":
    get_crypto_gdelt()
    get_cripto_notice()
    get_crypto_tweets()
    pass
