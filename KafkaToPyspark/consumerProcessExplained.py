from pyspark.ml import PipelineModel
from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import ALSModel


"""
Create a MongoClient object that represents the connection to the MongoDB server, 
localhost specifies that the server is running on the local machine and the 2701 
parameteris the defaukt port number for MongoDB to listen for connections 
(The port to be used to access this service)
"""
client = MongoClient('localhost', 27017) 

"""
 This line selects a specific database, bigdata_project, from the MongoDB server
If it doesn't exist, it will be created when you insert data.
""" 
db = client['bigdata_project'] 

"""
This line selects a collection from the database bigdata_project. 
If it doesn't exist, if will be created when you insert data.
"""
collection = db['User-item ratings'] 



# Assuming you have a SparkSession already created - Building the Spark session
spark = SparkSession.builder \
	.appName("Predict ratings") \
	.getOrCreate()


"""
Sparksession is a class that represents an entry point API for Spark functionality.
builder is a method of the sparksession class is used to return a builder object that 
can be used to configure various spark properties
appname() is a method of the builder object that is used to specify the application
name, it is used to identify the application in the spark cluster
getOrCreate() is a method of the builder object that retrieves an existing sparksession
and if it doesn't exist it creates one, ensuring one spark session is created per 
application
--> the sparksession object is an entry point to interacting with spark and
performing various operatiions on distributed data
"""


# Load the model
loadedALSModel = ALSModel.load("als_model")


def inputPreprocessing(text):
    return
    


# Kafka Consumer
consumer = KafkaConsumer( # A class that allows you to consume messages from a Kafka cluster
    'numtest', # Specify the topic to consume from
    bootstrap_servers=['localhost:9092'], # Specify the list of kafka broker addresses that the consumer should connect to
    auto_offset_reset='earliest', # The consumer will start consuming topics from the earlires possible offset, this is specified 
								  # to determine what happens when the consumer starts consuming messages for the first time or 
                                  # when there is no valid offset 
    enable_auto_commit=True,      # Whether the consumer should commit offsets automaticall
    group_id='my-group',		# An identifier for which group the consumer belongs to, groups help multiple consumers work 
    							# together to consume messages from a topic
    value_deserializer=lambda x: loads(x.decode('utf-8'))) # is everything sent should be an array of bytes? 
								# This is a function that deserializes the message value, the default is to return the message as a byte array
                                # the loads function is used to desrialize the message, in this case it converts a JSON string into
                                # a python object	


for message in consumer:
    tweet = message.value[-1]  # get the Text from the list
    preprocessed_tweet = inputPreprocessing(tweet)
    # print("-> tweet : ", tweet)
    # print("-> preprocessed_tweet : ", preprocessed_tweet)
    # Create a DataFrame from the string
    data = [(preprocessed_tweet,),]  
    data = spark.createDataFrame(data, ["Text"])
    # Apply the pipeline to the new text
    processed_validation = pipeline.transform(data)
    prediction = processed_validation.collect()[0][6]

    print("-> Tweet:", tweet)
    print("-> preprocessed_tweet : ", preprocessed_tweet)
    print("-> Predicted Sentiment:", prediction)
    print("-> Predicted Sentiment classname:", class_index_mapping[int(prediction)])
    

    # Prepare document to insert into MongoDB
    tweet_doc = {
        "tweet": tweet,
        "prediction": class_index_mapping[int(prediction)]
    }

    # Insert document into MongoDB collection
    collection.insert_one(tweet_doc)

    print("/"*50)