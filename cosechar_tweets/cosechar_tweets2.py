import couchdb #Libreria de CouchDB (requiere ser instalada primero)
from tweepy import Stream #tweepy es la libreria que trae tweets desde la API de Twitter (requiere ser instalada primero)
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json #Libreria para manejar archivos JSON


ckey = "b0hhIO0RfXbsgPjjBSlcAOEkB"
csecret = "3825kTPBIf0CkktKEZ1Q5aMjJe9HqMq8RD9P3sX0Tz1gf0p0dd"
atoken = "1268502774-9G1na0czUOXCbiac8hs3S31c976JHExouRJGm5c"
asecret = "PAhtCrPxacjS2AmTyIOCSKmGfPQzuF9jll07n2bT7ptgr"

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
    db = server.create('bdd2')
except:
    #Caso contrario solo conectarse a la base existente
    db = server['bdd2']
    
#Aqui se define el bounding box con los limites geograficos donde recolectar los tweets
twitterStream.filter(locations=[19.08,59.45,31.59,70.09])
#twitterStream.filter(track = ["Comba","comba"])
