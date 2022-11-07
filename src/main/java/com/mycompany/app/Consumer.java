package com.mycompany.app;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

    /**
 * Class: 44-517 Big Data
 * Author: Gabriel ("Makerspace Manager" Solomon Holland)
 * Description: This is a kafka multi channel consumer project
 * Due: November 4, 2022
 * I pledge that I have completed the programming assignment independently.
   I have not copied the code from a student or any source.
   I have not given my code to any other student.
   I have not given my code to any other student and will not share this code
   with anyone under any circumstances.
*/
    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        String bootstrapServers = "localhost:9092";        

        // create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dreloemisleadme");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //subscription (wow even open source projects are making you subscribe, disgusting)
        consumer.subscribe(Arrays.asList("Rack1Temps"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records)
            {
                System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
            }
        }

    }
    
}
