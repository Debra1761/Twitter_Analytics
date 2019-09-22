from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import time


access_token = "68979886-IzdibLmYAx39y8PLNWA7kLPKl2rTDlLPCnf557I45"
access_token_secret =  "tjVsF4mx4vS9JO0hPcS7b8qoP7oIZK1A8nX0aMwhkNEDG"
consumer_key =  "jjSz1RE4ftTNmqB1XuUTM22Fc"
consumer_secret =  "5SNWrhQStzMp3UDwVY7YGuEofQ4QOBBP4rOo4hGnhKpdFQoVi9"

stream_keywords = ["tsunami", "natural disasters", "volcano", "tornado",
                   "avalanche", "earthquake", "blizzard", "drought", "woodland fire",
                   "tremor", "dust storm", "magma", "twister", "windstorm", "heat wave",
                   "cyclone", "forest fire", "flood", "fire", "hailstorm", "lava", "lightning",
                   "high-pressure", "hail", "hurricane", "seismic", "erosion", "whirlpool",
                   "richter scale", "whirlwind", "cloud", "thunderstorm", "gale", "blackout",
                   "gust", "volt", "snowstorm", "rainstorm", "storm", "nimbus", "violent storm",
                   "sandstorm", "casualty", "beaufort scale", "fatal", "fatality", "cumulonimbus",
                   "destruction", "cataclysm", "damage", "wind scale", "permafrost", "disaster"]


class StdOutListener(StreamListener):
    def on_data(self, data):
        data = data.encode('utf-8')
        producer.send_messages("tweetstream", data)

        return True
    def on_error(self, status):
        print (status)


success = False
kafka = None
while success == False:
    time.sleep(1)
    try:
        kafka = KafkaClient("kafka:9092")
        producer = SimpleProducer(kafka)
        l = StdOutListener()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, l)
        stream.filter(track=stream_keywords)
        success = True
    except:
        print("producer error")

