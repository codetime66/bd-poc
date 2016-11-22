package org.bdpoc.kafka.partition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class FileProducer {

    public static void main(String[] argv)throws Exception {
        if (argv.length != 2) {
            System.err.println("Please specify 2 parameters: <topic_name> <file> ");
            System.exit(-1);
        }
        String topicName = argv[0];
        BufferedReader in =  new BufferedReader(new FileReader(new File(argv[1])));        

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,AssetTypePartitioner.class.getCanonicalName());
        configProperties.put("partitions.0","SWAP");
        configProperties.put("partitions.1","FUTURES");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        
        String str=null;
        while ( (str = in.readLine()) != null ) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, String.valueOf(str));
            producer.send(rec, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("Message sent to topic ->" + metadata.topic()+ " ,parition->" + metadata.partition() +" stored at offset->" + metadata.offset());
                }
            });
        }
        in.close();
        producer.close();
    }
}
