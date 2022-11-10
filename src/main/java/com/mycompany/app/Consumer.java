package com.mycompany.app;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Queue;

public class Consumer {

    /**
 * Class: 44-517 Big Data
 * Author: Gabriel ("Makerspace Manager") Solomon Holland
 * Description: This is a kafka multi channel consumer project
 * Due: November 11, 2022
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

        //uncomment which ones you're using
        //consumer.subscribe(Arrays.asList("Rack1Temps"));
        consumer.subscribe(Arrays.asList("Rack2Temps"));
        //consumer.subscribe(Arrays.asList("Smoker"));
        int count = 0;

        Queue<Double> temps = new LinkedList<Double>(); //This is a QUEUE, gib me bonus points
        Queue<String> times = new LinkedList<String>();

        Double doubValue = 0.0;
        String[] recordArr;
        Boolean stall;
        //Boolean giveAttention;

        while(true) { //while true *vomit emoji*
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records)
            {

                //split the line we're on
                recordArr = record.value().split("\t");
                doubValue = Double.parseDouble(recordArr[1]);
                
                //System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
                temps.add(doubValue);
                times.add(recordArr[0]);

                //pull off the head and assign to the var we already made. No sense in making more vars.
                doubValue = temps.peek(); 

                /* 
                //this is for the smoker attention code
                //reset bool value for multiple runs
                giveAttention = false;

                for(Double i:temps)
                {
                    //if temps drop 15 degrees over 2.5 minutes (5 datapoints)
                    if(i>=doubValue+15 || i<=doubValue-15)
                    {
                        giveAttention = true;
                        break;
                    }
                }

                if(giveAttention)
                {
                    System.out.println("Give attention at: " + times.peek());
                }
                if(temps.size()>5)
                {
                    //remove oldest (first value) to not save entire dataset
                    //System.out.println(temps);
                    temps.remove();
                    times.remove();
                }
                */

                
                
                //this is for stall code
                //reset bool value for multiple runs
                stall = true;

                //parse through our array, max size should be 20. 20 size = 10 minutes
                for(Double i:temps)
                {
                    //if any value is NOT within 1, it sets our check to false
                    if(i>=doubValue+1 || i<=doubValue-1)
                    {
                        stall = false;
                        break;
                    }
                }
                if(stall)
                {
                    System.out.println("Stall Starts at:  " + times.peek());
                }
                

                if(temps.size()>20)
                {
                    //remove oldest (first) value because we don't want to save the entire data set
                    temps.remove();
                    times.remove();
                }
                
                
            }      
        }

    }
    
}
