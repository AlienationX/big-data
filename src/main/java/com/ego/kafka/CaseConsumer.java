package com.ego.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;


public class CaseConsumer {

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final Boolean isAsync;


    private CaseConsumer(String topic, Boolean isAsync) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "hadoop-dev03:9092,hadoop-dev04:9092,hadoop-dev05:9092");
        kafkaProps.put("key.deserializer", IntegerDeserializer.class.getName());
        kafkaProps.put("value.deserializer", StringDeserializer.class.getName());
        kafkaProps.put("group.id", "test");

        this.consumer = new KafkaConsumer<>(kafkaProps);
        this.topic = topic;
        this.isAsync = isAsync;
    }


    private void receiveMessage() {
        consumer.subscribe(Collections.singletonList(this.topic));
        // consumer.subscribe(Arrays.asList("foo", "bar"));
        // consumer.subscribe(Pattern);


        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            // consumer.seekToBeginning(consumer.partitionsFor("test"));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.printf("GroupId %s topic %s received message: from partition %s, (%s, %s) at offset %s", record.topic(), record.topic(), record.partition(), record.key(), record.value(), record.offset());
                if (this.isAsync) {
                    consumer.commitAsync();
                } else {
                    consumer.commitSync();
                }
            }
            System.out.println("GroupId " + " finished reading " + records.count() + " messages");
        }

    }


    public static void main(String[] args) {
        CaseConsumer demo = new CaseConsumer("test", false);
        demo.receiveMessage();
    }
}