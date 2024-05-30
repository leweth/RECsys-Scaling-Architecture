from pyspark.ml.recommendation import ALSModel
from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.sql import SparkSession
import os



# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['recommendation_system']
collection = db['recommendations']

# Spark Session
spark = SparkSession.builder \
    .appName("recommendation_system") \
    .getOrCreate()

# Load the ALS model
als_model = ALSModel.load("als_model")

# Kafka Consumer
consumer = KafkaConsumer(
    'numtest',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

def recommend_for_user(user_id, num_recommendations=10):
    user_df = spark.createDataFrame([(user_id,)], ["userIDIndex"])
    recommendations = als_model.recommendForUserSubset(user_df, num_recommendations)
    return recommendations

for message in consumer:
    record = message.value
    
    # Check if the record is a header
    if record[0] == 'rating':
        print("Skipping header row")
        continue

    # Ensure correct parsing of incoming data
    try:
        rating = float(record[0])
        userID = record[1]
        productID = record[2]
        userIDIndex = float(record[3])
        productIDIndex = float(record[4])
    except ValueError as e:
        print(f"Error parsing record: {record}, Error: {e}")
        continue
    
 # Generate recommendations for the user
    recommendations = recommend_for_user(userIDIndex)
    
    # Print schema and content for debugging
    recommendations.printSchema()
    recommendations.show(truncate=False)

    # Check if recommendations DataFrame contains expected columns
    if 'recommendations' not in recommendations.columns:
        print(f"Unexpected DataFrame schema: {recommendations.schema}")
        continue

    # Collect the recommendations
    recommendations_list = recommendations.select("userIDIndex", "recommendations").collect()

    for rec in recommendations_list:
        user = rec.userIDIndex
        products = [row.productIDIndex for row in rec.recommendations]

        # Prepare document to insert into MongoDB
        recommendation_doc = {
            "userIDIndex": user,
            "recommendedProductIDs": products
        }

        # Insert document into MongoDB collection
        collection.insert_one(recommendation_doc)

    print("Recommendations for user:", user)
    print("Recommended products:", products)
    print("/"*50)
