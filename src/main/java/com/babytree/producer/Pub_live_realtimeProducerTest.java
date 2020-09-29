package com.babytree.producer;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;
import java.util.Properties;

/**
 * 直播测试
 *
 * @author chenwu on 2020.9.29
 */
public class Pub_live_realtimeProducerTest {

    public static void main(String[] args) {
        int i = 0;
        if(args.length>=1){
            i = Integer.parseInt(Optional.of(args[0]).orElse("0"));
        }
        String topicName = "pub_live_distribute";
        String bootStrapServerConfig = "172.26.6.87:9095,172.26.6.88:9095,172.26.6.89:9095";
        Gson gson = new Gson();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.34.117:9095,172.26.34.118:9095,172.26.34.119:9095");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServerConfig);
        KafkaProducer producer = new KafkaProducer(properties);

        while(true){
            PubLiveModel liveModel = new PubLiveModel();
            liveModel.setDiscussion_id((long)i);
            liveModel.setAppointment_start_ts("2020-09-29 15:47:32");
            liveModel.setPv_total(i+100);
            liveModel.setVideo_type(i+2);
            producer.send(new ProducerRecord(topicName, JSON.toJSONString(liveModel)));
            i+=1;
            if(i%10==0){
                try{
                    Thread.sleep(5000);
                }catch(InterruptedException e){
                    e.printStackTrace();
                    break;
                }
            }
            //发送一条id为空的数据
            liveModel.setDiscussion_id(null);
            producer.send(new ProducerRecord(topicName,gson.toJson(liveModel)));
        }
    }
}
