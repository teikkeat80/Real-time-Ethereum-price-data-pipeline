# Real-time-Ethereum-price-data-pipeline
## Project Summary
This project uses technologies such as Apache Kafka and PySpark (Python API for Apache Spark) to build a real-time ETL Ethereum price data pipeline. The data pipeline streams Ethereum price data from EtherScan API and store it in either a CSV file or a PostgreSQL database for further real-time analysis. This data pipeline can be customised and scale up to handle large volumes of data from other EtherScan APIs or data sources.

The current setting of the data pipeline:
- Runs on local machine
- Kafka broker(s): 1, Replicate(s): 1, Partition(s): 1

The graphical representation of the data pipeline is as below:
![Screenshot 2023-04-03 at 17 17 51](https://user-images.githubusercontent.com/83192718/229586180-564259be-ff12-4ce5-94a1-5c6d0832b6d7.png)

## Running Instructions
1. Open terminal and navigate to kafka installation directory (e.g. '/usr/local/kafka/3.4.0/')
2. Start the ZooKeeper server by running the command `bin/zookeeper-server-start.sh config/zookeeper.properties`. Keep the terminal open.
3. Open another terminal and repeat step 1, then run the command `bin/kafka-server-start.sh config/server.properties`. Keep this terminal open as well.
<img width="1429" alt="Screenshot 2023-04-03 at 18 43 44" src="https://user-images.githubusercontent.com/83192718/229586744-8738bd6f-0730-425f-8df0-7079213a1a53.png">

4. Open another terminal and repeat step 1, run the command `bin/kafka-topics.sh —create —bootstrap-server <kafka-server IP & port> —replication-factor 1 —partitions 1 —topic <topic-name>` (Replace the desired parameters in `<topic-name>` and `<kafka-server IP & port>`. For this particular project, the topic name will be 'eth-price', and kafka server is 'localhost:9092')
5. Navigate to your PostgreSQL installation directory bin and start the postgreSQL server. Then use command lines like `createdb`, `createuser`, etc., to configure your postgreSQL properties. Visit https://www.postgresql.org/docs/ for detailed instructions.
6. Install necessary python packages (kafka-python, pyspark, etc.) by running `pip install <python-packages>`
7. Run producer.py to start Kafka Producer
8. Run csv_consumer.py for writing to CSV file
9. Run spark_consumer.py for writing to PostgreSQL Database using jdbc sink method

![Screenshot 2023-04-03 at 18 46 55](https://user-images.githubusercontent.com/83192718/229587464-e7c34ac7-ae33-4c7f-9672-50d1363269b6.png)

![Screenshot 2023-04-03 at 18 48 06](https://user-images.githubusercontent.com/83192718/229587558-690a5ab3-51ad-4d0e-9317-511b50c05289.png)

![Screenshot 2023-04-03 at 18 46 28](https://user-images.githubusercontent.com/83192718/229587435-78938b35-c02a-4ce6-aad1-816375c5b41f.png)

**Reminder: Use the .env.example file to create your own .env file for your API key, Kafka topic name, server & ports and PostgreSQL Database configurations.**

### Pre-requisites
- Java
- Apache Kafka (version 2.6 or later)
- EtherScan API key
- PostgreSQL Database
- Apache Spark (version 2.4 or later)
- Python 3
