package ru.sadikov.kafkatest.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties properties = new Properties();

        String bootstrap = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //Тут количество n раза пока сам поставил 10
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "{a,b,c "+ LocalDateTime.now()+"}";
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);
            logger.info("Key:"+key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("received new metadata\n" + "Topic:" + recordMetadata.topic() + "\n" +

                                "Partition:" + recordMetadata.partition() + "\n" +
                                "offset:" + recordMetadata.offset() + "\n" +
                                "TimeStamp:" + recordMetadata.timestamp()
                        );

                    } else {
                        logger.error("Error while", e);

                    }
                }
            }).get();
        }
            producer.flush();
            producer.close();
        }

}
