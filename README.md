# RealTime-Election-Voting
Lets hold an interesting election voting system in realtime using various Data Engineering technologies such as Apache Kafka, Apache Spark and witness it on a dashboaard using Streamlit

## Project WorkFlow 
![image](https://github.com/Anannya-M/RealTime-Election-Voting/assets/85965261/90f0dd2e-6ac1-4166-a19b-35e77eefcdcd)

The project workflow is as follows:
## 1. Data Generation
   First of all, Postgres is connected and 3 tables are created : candidates, voters and votes. Then fake data is generated for both the candidates and the voters list using    the 'Faker' Python library and the randomuserAPI.
      
## 2. Data Produced into Kafka Server
   Then a Kafka topic is created and the the data is produced into the kafka server.

## 3. Voting Process
   Here, a kafka consumer is created which sonsumes the data from the kafka server and carries out the voting process where each voter polls a vote to one of the 3        candidates.

## 4. Data Transformation Using  Spark Streaming
   Further the produced data is read by the SparkSession thorugh readStream() and are transformed and sql operations performed using SparkSQL and streaming procedures.
   After all the transformations are done, they are stored back into the kafka.

## 5. Data Visualization Using Streamlit
   Finally, all the transformed data is consumed again and now the data is visualized using the bar charts and pie charts in a dashboard depicting the vote statistics of ech candidate.

### We can witness the whole voting process being carried out on the streamlit dashboard, with a refreshing interval of 10 seconds and the data being updated. 
