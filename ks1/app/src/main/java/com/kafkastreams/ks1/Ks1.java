package com.kafkastreams.ks1;

import com.github.javafaker.File;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Ks1 {

    public static Properties loadProperties() throws Exception{

        Properties props = new Properties();
        InputStream is = new FileInputStream("configuration/dev.properties");
        props.load(is);
        return  props;

    }

    public static void main(String[] args) throws Exception {
        System.out.println("My First Stream!!!");

        Properties props= loadProperties();
        System.out.println(props);

        final String inputTopic = props.getProperty("input.topic.name");
        final String outputTopic = props.getProperty("output.topic.name");

        StreamsBuilder sb = new StreamsBuilder();
        KStream<String,String> ks1 = sb.stream("inputTopic");
        ks1.foreach((k, v) -> System.out.println("Key= " + k + " Value= " + v));
        System.out.println("My First Stream!!!!!!!!!!!!!!!!!");
        Topology  tpg = sb.build();
        KafkaStreams ks = new KafkaStreams(tpg,props);
        ks.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            ks.close();
        }));

        System.out.println("My First Streammmmmmmm!!!");

    }

}
