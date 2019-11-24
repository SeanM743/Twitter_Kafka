from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

consumer_key= 'JvqSWMz4BiaNbGBpTkodViPRp'
consumer_secret= 'CCGys1e9BIlsRNiXrquFD106kBnqqh3tkue94rNEP47GvZtwZf'
access_token= '1192207247008501766-qZWORwlb5RqwQgcOJcWYRsVccSCpNA'
access_token_secret= 'r0tRfEUv93TMme6cMzOPjthsmoW3AF1S2adpZbO3J9Vxs'

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["chicken sandwich"])