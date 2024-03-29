import couchdb #Libreria de CouchDB (requiere ser instalada primero)
from tweepy import Stream #tweepy es la libreria que trae tweets desde la API de Twitter (requiere ser instalada primero)
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json #Libreria para manejar archivos JSON


ckey = "lzuZ0mSkmsn8VJwS2QWoNpoQ8"
csecret = "KrOi8uQK74npe7oPmXqesGiBENsvWPAxzYbSVWLdyGlD2JSbvw"
atoken = "831090241-NVFwVkZ3otRVkqVT6Lwvrbp3w92mjTiJroQEzdDS"
asecret = "J61DAI7azJJ2P7QrbxLqddHhSRNW68NXxE7GUBmbQRDwK"

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
twitterStream.filter(locations=[3.5,48.4,51.8,71.5])
#twitterStream.filter(track = ["HUAWEY","Huawey","huawey"])
