package ru.sadikov.kafkatest.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import java.net.UnknownHostException;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) throws UnknownHostException{
        MongoClient mongo = new MongoClient("localhost", 27017);
        MongoDatabase db = mongo.getDatabase("messages");
//---------- Creating Collection -------------------------//
        MongoCollection<Document> table = db.getCollection("message");
//---------- Creating Document ---------------------------//

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        Properties properties = new Properties();
        String bootstrapService="127.0.0.1:9092";
        String groupID = "my-fourth-application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapService);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while (true){
           ConsumerRecords<String,String> records =
                   consumer.poll(Duration.ofMillis(100));
           for (ConsumerRecord<String,String> record:records){
               logger.info("Key: "+ record.key()+
                       "Value: " + record.value());
               Document doc = new Document("mess",record.value());

                table.insertOne(doc);
               logger.info("Partition:" + record.partition()+ " Offset: " + record.offset());           }
        }
    }
}
