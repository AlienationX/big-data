package com.ego.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;


public class CaseProducer {

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;


    private CaseProducer(String topic, Boolean isAsync) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "hadoop-dev03:9092,hadoop-dev04:9092,hadoop-dev05:9092");
        kafkaProps.put("key.serializer", IntegerSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(kafkaProps);
        this.topic = topic;
        this.isAsync = isAsync;
    }


    private void sendMessage() {
        int numRecords = 10;
        Date flagTime = new Date();

        int messageKey = 0;
        String messageVal = "Message_" + messageKey + "_" + flagTime.toString();

        // 直接发送消息到kafka，不关心发送的结果
        // producer.send(new ProducerRecord<>(this.topic, messageKey, messageVal));

        if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<>(
                    topic,
                    messageKey,
                    messageVal
            ), new CaseProducerCallback(flagTime.getTime(), messageKey, messageVal));
            // 直接new Callback好像不能传递参数
            // ), new Callback() {
            //     @Override
            //     public void onCompletion(RecordMetadata metadata, Exception e) {
            //         if (metadata != null) {
            //             System.out.println("Sent success. offset: " + metadata.offset());
            //         } else {
            //             e.printStackTrace();
            //         }
            //     }
            // });
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<>(
                        topic,
                        messageKey,
                        messageVal)
                ).get();
                System.out.println("Sent message: (" + messageKey + ", " + messageVal + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Producer sent " + numRecords + " records successfully");
    }


    public static void main(String[] args) {
        CaseProducer demo = new CaseProducer("test", false);
        demo.sendMessage();
    }
}


