# Example for a data pipeline

## Overview
The data pipeline reads a continous stream of tweets from the twitter stream API, stores each message into Kafka and retrieves it again from Kafka.

## Components
The components of the data pipeline are
* Kafka producer, reading twitter messages and writing them to Kafka
* Kafka (together with Zookeeper)
* Kafka consumer, reading messages from Kafka and printing them

## How to run
* Get a twitter account and register a dummy app at `https://apps.twitter.com/` in order to get Twitter API credentials. Set the Twitter API credentials in the producer properties file `twitter.properties`.
* Build the docker images for the producer. In the `producer` folder, run the statement
  ```
  docker build . -t producer
  ```
* Build the docker images for the consumer. In the `consumer` folder, run the statement
  ```
  docker build . -t consumer
  ```
* Determine the IP address of your machine in the local network and set it in the `docker-compose.yaml` file for the parameter "KAFKA_ADVERTISED_HOST_NAME".
* Now you are ready to run the whole pipeline. In the `pipeline` folder, execute the statement
  ```
  docker-compose up
  ```
  You should see log messages from kafka, zookeeper, producer and consumer, and then a stream of twitter messages printed by the consumer. Stop the pipeline with CTRL-C.
* When running the pipeline in detached mode, it can be stopped with "docker-compose down": 
  ```
  docker-compose up -d
  [...]
  docker-compose down
  ```
