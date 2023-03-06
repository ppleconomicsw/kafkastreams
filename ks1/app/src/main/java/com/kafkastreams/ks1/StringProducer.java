package com.kafkastreams.ks1;

        import java.io.FileInputStream;
        import java.io.InputStream;
        import java.util.Properties;
        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.clients.producer.Producer;
        import org.apache.kafka.clients.producer.ProducerRecord;
        import com.github.javafaker.Faker;
        import java.util.UUID;
        import java.util.concurrent.Future;
        import org.apache.kafka.clients.producer.RecordMetadata;

public class StringProducer {



    public static Properties laodProperties() throws Exception{
        Properties props = new Properties();
        InputStream is = new FileInputStream("configuration/dev.properties");
        props.load(is);
        return props;
    }




    public static void main(String[] args) throws Exception {

        Faker faker =  new Faker();
        Properties props = laodProperties();
        String in_topic=props.getProperty("input.topic.name");



        while(true) {
            String uuid= UUID.randomUUID().toString();
            String fakeStr=faker.chuckNorris().fact();

            Producer<String, String> strproducer = new KafkaProducer<>(props);
            strproducer.send(new ProducerRecord<String, String>(in_topic, uuid, fakeStr), (recordMetadata, exception) -> {
                if (exception == null) {
                    System.out.println(uuid + "---------" + fakeStr);
                    System.out.println("Record written to offset " +
                            recordMetadata.offset() + " timestamp " +
                            recordMetadata.timestamp());



                } else {
                    exception.printStackTrace(System.err);
                }

            });

            Thread.sleep(1000);
        }

//        try (KafkaProducer producer = new KafkaProducer<String, String>(props)) {
//            Faker faker = new Faker();
//            while (!closed) {
//                try {
//                    Object result = producer.send(new ProducerRecord<>(
//                            this.topic,
//                            faker.chuckNorris().fact())).get();
//
//                    producer.send(new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), faker.chuckNorris().fact()), (recordMetadata, exception) -> {
//
//                        if (exception == null) {
//                            System.out.println("Record written to offset " +
//                                    recordMetadata.offset() + " timestamp " +
//                                    recordMetadata.timestamp());
//                        } else {
//                            System.err.println("An error occurred");
//                            exception.printStackTrace(System.err);
//                        }
//                    });
//                    Thread.sleep(5000);
//                } catch (InterruptedException e) {
//                }
//            }
//        } catch (Exception ex) {
//            System.out.println(ex.toString());
//        }
//    }

    }
}
