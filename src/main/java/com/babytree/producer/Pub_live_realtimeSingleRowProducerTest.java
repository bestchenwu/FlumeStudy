package com.babytree.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 单条消息发送
 *
 * @author chenwu on 2020.10.22
 */
public class Pub_live_realtimeSingleRowProducerTest {

    public static void main(String[] args) {
        String content = args[0];
        String topicName = "pub_live_distribute";
        String bootStrapServerConfig = "172.26.6.87:9095,172.26.6.88:9095,172.26.6.89:9095";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.34.117:9095,172.26.34.118:9095,172.26.34.119:9095");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServerConfig);
        KafkaProducer producer = new KafkaProducer(properties);
        producer.send(new ProducerRecord(topicName,content));
        producer.close();
    }
}
