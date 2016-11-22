# bd-poc
big data poc


1) Visit the Kafka download page to install the most recent version

2) Extract the binaries into a software/kafka folder

3) Change your current directory to point to the new folder.

4) Start the Zookeeper server by executing the command:
      bin/zookeeper-server-start.sh config/zookeeper.properties

5) Start the Kafka server by executing:
      bin/kafka-server-start.sh config/server.properties

6) Create a test topic that you can use for testing:
      bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <topic_name>

7) Compile the code and create a fat JAR with the command:
       mvn clean compile assembly:single

8) Start the consumer:
       java -cp target/KafkaPOC-1.0-SNAPSHOT-jar-with-dependencies.jar org.bdpoc.kafka.simple.Consumer <topic_name> group1

9) Start the producer:
       java -cp target/KafkaPOC-1.0-SNAPSHOT-jar-with-dependencies.jar org.bdpoc.kafka.simple.Producer <topic_name>

10) Enter a message in the producer console and check to see whether that message appears in the consumer. Try a few messages.

11) Type exit in the consumer and producer consoles to close them.

# a different test: usage of partitions and queuing the message processing 

# create a new topic, with 3 partitions:
       bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic part-demo

# Start three consumers using the same group ID that listen for messages published to the topic:

       java -cp target/KafkaPOC-1.0-SNAPSHOT-jar-with-dependencies.jar org.bdpoc.kafka.partition.FileConsumer part-demo group1

# use de producer to send a file (several times):
       java -cp target/KafkaPOC-1.0-SNAPSHOT-jar-with-dependencies.jar org.bdpoc.kafka.partition.FileProducer part-demo in/test3.txt 
