package org.bdpoc.kafka.simple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

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

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        String str=null;
        while ( (str = in.readLine()) != null ) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, String.valueOf(str));
            producer.send(rec);
        }
        in.close();
        producer.close();
    }
}
