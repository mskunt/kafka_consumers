# kafka_consumers
Consumers for pool data from Kafka.  
In my CDC project , I used debezium and Kafka for transfering data from Postgresql to SingleStore Database.  
I cant find an good example for parsing debezium messages from Kafka.  
After I developed my consumer with python, I decide to share the codes for everyone.  
The code is almost same for target database, for the beginning I will add only for Singlestore database. Singlestore is based on Mysql. So u can use my code for Mysql.  

