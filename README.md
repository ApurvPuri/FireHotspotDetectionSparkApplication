Here multiple Apache Kafka producers will be implemented to simulate the real-time streaming of the data which will be processed by Apache Spark Streaming client and then inserted into MongoDB. Simulating real-time data using Apache Kafka Producers.

1. Kafka Producers

a. Event Producer 1: A python program that loads all the data from climate_streaming.csv and randomly feed the data to the stream every 5 seconds. Additional information such as sender_id and created_time is added. 
b. Event Producer 2: A python program that loads all the data from hotspot_AQUA_streaming.csv and randomly feed the data to the stream every 10 - 30 seconds. AQUA is the satellite from NASA that reports latitude, longitude, confidence and surface temperature of a location. Additional information such as sender_id and created_time is added. 
c. Event Producer 3: A python program that loads all the data from hotspot_TERRA_streaming.csv and randomly feed the data to the stream every 10 - 30 seconds. TERRA is another satellite from NASA that reports latitude, longitude, confidence and surface temperature of a location. Additional information such as sender_id and created_time is added.  

2. Stream Processing using Apache Spark Streaming. 

a. Streaming Application: A streaming application in Apache Spark Streaming which has a local streaming context with two execution threads and batch interval of 10 seconds. The streaming application will receive streaming data from all three producers. If the streaming application has data from all or at least two of the producers, data processing as follows: - streams joined based on the location (i,e, latitude and longitude) and the data model developed. - Finding If two locations are close to each other or not by implementing the geo-hashing algorithm (precision 5 used). The precision determines the number of characters in the Geohash. - If we receive the data from two different satellites AQUA and TERRA for the same location, then the ‘surface temperature’ and ‘confidence’ averaged. - If the streaming application has the data from only one producer (Producer 1), it implies that there was no fire at that time and we can store the climate data into MongoDB straight away.

3. Data Visualisation using MatPlotLib 

a. Streaming data visualization: i. For the incoming climate data, the line graph of air temperature against arrival time is plotted. Interesting points such as maximum and minimum values labeled. 
b. Static data visualization: Python programs using pymongo to get the data from the MongoDB collection(s) created and the following visualizations performed. i. Records with the top 10 number of fires. A bar chart with time as the x-axis and number of fires as the y-axis. ii. Plotted fire locations in the map with air temperature, surface temperature, relative humidity and confidence
