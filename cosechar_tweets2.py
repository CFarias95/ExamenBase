import couchdb #Libreria de CouchDB (requiere ser instalada primero)
from tweepy import Stream #tweepy es la libreria que trae tweets desde la API de Twitter (requiere ser instalada primero)
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json #Libreria para manejar archivos JSON


ckey = "GpP3M0eHBVWDwSJTcruANgTvs"
csecret = "Y0lgWMEsaKkHo01aXRyOddbtqgLcHivn2aA0ljaUazILcPLFyY"
atoken = "831090241-9PMhSZqLGzuPOPMD56eLoGZ1JMllLHKn3PaSINu4"
asecret = "B0ePrHlvzC0lgNcmFYzUfvfDYUQg2aV5iiRniP3YrBk3O"

class listener(StreamListener):
    
    def on_data(self, data):
        dictTweet = json.loads(data)
        try:
            dictTweet["_id"] = str(dictTweet['id'])
           
            doc = db.save(dictTweet) #Aqui se guarda el tweet en la base de couchDB
            print ("Guardado " + "=> " + dictTweet["_id"])
        except:
            print ("Documento ya existe")
            pass
        return True
    
    def on_error(self, status):
        print (status)
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())

#Setear la URL del servidor de couchDB
server = couchdb.Server('http://localhost:5984/')
try:
    #Si no existe la Base de datos la crea
    db = server.create('bdd1')
except:
    #Caso contrario solo conectarse a la base existente
    db = server['bdd1']
    
#Aqui se define el bounding box con los limites geograficos donde recolectar los tweets
twitterStream.filter(locations=[-123.6,28.0,-67.2,48.6])
#twitterStream.filter(track = ["HUAWEY","Huawey","huawey"])