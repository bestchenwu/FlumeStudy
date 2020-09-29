package com.babytree.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka 0.8生产者测试
 *
 * @author chenwu on 2020.9.29
 */
public class KafkaProducerTest {

    public static void main(String[] args) {
        String topicName = args[0];
        String bootStrapServerConfig = args[1];
        Gson gson = new Gson();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.34.117:9095,172.26.34.118:9095,172.26.34.119:9095");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServerConfig);
        KafkaProducer producer = new KafkaProducer(properties);
        int i = 0;
        while(true){
            MacQueryModel queryModel = new MacQueryModel();
            queryModel.setMac("test"+i);
            queryModel.setRecent_3_query("query"+i);
            producer.send(new ProducerRecord(topicName,gson.toJson(queryModel)));
            i+=1;
            if(i%10==0){
                try{
                    Thread.sleep(5000);
                }catch(InterruptedException e){
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
