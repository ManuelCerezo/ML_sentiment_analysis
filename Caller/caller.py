#CALLER PYTHON REQUIREMENTS
import datetime
from bs4 import BeautifulSoup
import requests
import tweepy
import socket
import sys
#CALLER TWITTER METADATA

from config_twitter_access import *
# auth = tweepy.OAuthHandler(TWITTER_API_KEY,TWITTER_API_KEY_SECRET)
# auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY,TWITTER_API_KEY_SECRET)
# auth.set_access_token(TWITTER_ACCESS_TOKEN,TWITTER_ACCESS_TOKEN_SECRET)
# api = tweepy.API(auth)
client = client = tweepy.Client(TWITTER_BAREER_TOKEN)


#CALLER METADATA
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
BASE_URL = 'https://coinmarketcal.com/en/news?page={}'
GELPH_API_URL = 'https://api.gdeltproject.org/api/v2/doc/doc?query={} sourcelang:eng&maxrecords=250&timespan=1day&format=JSON&sort=datedesc'

#SOCKET METADATA
TCP_IP = "localhost"
TCP_PORT = 10002
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT)) #Abrimos la conexion handshake
s.listen(1)

print("Waiting for TCP connection...")
conn, addr = s.accept()

def get_cripto_notice():
    cantidad = 0
    num_page = 355
    active_while = True
    global conn
    array = []
    while (active_while):
        sourceCode = requests.get(url= BASE_URL.format(num_page), headers= HEADERS, timeout=5).text
        sourceCode = BeautifulSoup(sourceCode, 'html.parser')
        if not sourceCode.find('h5',class_='card__title mb-0'):
            active_while = False
        for notice , fecha in zip(sourceCode.find_all('h5',class_='card__title mb-0'),sourceCode.find_all('h5',class_='card__date')):
            #print(BASE_URL.format(num_page),";",notice.text,";",datetime.datetime.strptime(fecha.text, '%d %b %Y').date())
            send_tweets_to_spark(BASE_URL.format(num_page)+";"+notice.text+";"+str(datetime.datetime.strptime(fecha.text, '%d %b %Y').date()),conn)
            cantidad = cantidad + 1
        num_page = num_page + 1
    print('cantidad de noticias: ',cantidad)

def get_crypto_tweets():
    response = client.search_recent_tweets(' #cryptonews lang:en',max_results = 10, tweet_fields = ['created_at','lang'])
    
    for tweet in response.data:
        print(deEmojify(tweet.text))

def get_crypto_gdelt():
    queries =['(crypto OR cryptocurrencies)']
    cantidad = 0
    
    for query in queries:
        request = requests.get(url=GELPH_API_URL.format(query)).json()     
        for article in request['articles']:
            print(article['seendate'][0:4]+'-'+article['seendate'][4:6]+"-"+article['seendate'][6:8])

            print((article['url']+" ; "+article["title"]+" ; "+article['seendate']),'\n')
           # print(datetime.datetime.strptime(article['seendate'],'%Y%m%d %f %Z'))
            cantidad = cantidad +1

    # PARA TESTEAR
    # request = requests.get(url='https://api.gdeltproject.org/api/v2/doc/doc?query=ethereum%20sourcelang:eng%20&maxrecords=250&timespan=1day&sort=datedesc&format=JSON').json()
    # for request in request['articles']:
    #     print(request['title'],'\n')
        
    print('cantidad noticias: ',cantidad)

def deEmojify(inputString): #quitar emoji a tweets
    return inputString.encode('ascii', 'ignore').decode('ascii')

def send_tweets_to_spark(full_tweet, tcp_connection):
    try:
        print ("------------------------------------------")
        print("Text: " + full_tweet)
        print(tcp_connection.send(full_tweet.encode('utf-8')))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

if __name__ == "__main__":
    #get_crypto_gdelt()
    get_cripto_notice()
    #get_crypto_tweets()
    pass
