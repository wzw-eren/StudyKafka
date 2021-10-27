package com.wzw;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class HelloProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.137.128:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "hello", "hello");

        /**
         *
         * 如果发送消息，消息不指定key，那么我们发送的这些消息，会被轮训的发送到不同的分区。
         * 如果指定了key。发送消息的时候，客户端会根据这个key计算出来一个hash值，
         * 根据这个hash值会把消息发送到对应的分区里面。
         */

        //kafka发送数据有两种方式：
        //1:异步的方式。
        // 这是异步发送的模式
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    // 消息发送成功
                    System.out.println("消息发送成功");
                } else {
                    // 消息发送失败，需要重新发送
                }
            }

        });

        Thread.sleep(10 * 1000);

        producer.close();
    }

}
