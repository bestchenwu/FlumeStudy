package com.babytree.producer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * 单条消息发送
 *
 * @author chenwu on 2020.10.22
 */
public class Pub_live_realtimeSingleRowProducerTest {

    public static void main(String[] args) {
        Integer live_status = Integer.parseInt(args[0]);
        //String body = "{\"appointment_end_ts\":1603274580000000,\"appointment_start_ts\":1603274580000,\"business\":\"pregnancy\",\"comment_total\":0,\"comment_total_actual\":0,\"comment_total_base\":0,\"comment_user_actual\":0,\"comment_user_base\":0,\"comment_user_total\":0,\"discussion_id\":94754262,\"distribute_ts\":1603274580000,\"during\":1603277440000,\"end_ts\":1603277440000,\"follower_total\":0,\"follower_total_actual\":0,\"follower_total_base\":0,\"item_type\":1,\"live_status\":3,\"name\":\"爱动漫的初冬的雪111\",\"online_max_total\":3,\"online_max_total_actual\":3,\"online_max_total_base\":0,\"owner_id\":\"u12400672386\",\"owner_type\":1,\"praise_total\":0,\"praise_total_actual\":0,\"praise_total_base\":0,\"pv_total\":16,\"pv_total_actual\":6,\"pv_total_base\":0,\"question_total\":0,\"question_total_actual\":0,\"question_total_base\":0,\"scence_id\":6326,\"start_ts\":0,\"user_type\":0,\"uv_total\":4,\"uv_total_actual\":4,\"uv_total_base\":0,\"video_type\":6}";
        String body ="{\"appointment_end_ts\":0,\"appointment_start_ts\":0,\"business\":\"pregnancy\",\"comment_total\":8,\"comment_total_actual\":8,\"comment_total_base\":0,\"comment_user_actual\":1,\"comment_user_base\":0,\"comment_user_total\":1,\"discussion_id\":94770430,\"distribute_ts\":1603361388000,\"during\":371,\"end_ts\":1603361759000,\"follower_total\":0,\"follower_total_actual\":0,\"follower_total_base\":0,\"item_type\":1,\"live_status\":2,\"name\":\"\",\"online_max_total\":6,\"online_max_total_actual\":6,\"online_max_total_base\":0,\"owner_id\":\"u104889911791\",\"owner_type\":1,\"praise_total\":0,\"praise_total_actual\":0,\"praise_total_base\":0,\"pv_total\":13,\"pv_total_actual\":13,\"pv_total_base\":0,\"question_total\":0,\"question_total_actual\":0,\"question_total_base\":0,\"scence_id\":6398,\"start_ts\":1603361388000,\"user_type\":0,\"uv_total\":9,\"uv_total_actual\":9,\"uv_total_base\":0,\"video_type\":6}";
        String topicName = "pub_live_distribute";
        String bootStrapServerConfig = "172.26.6.87:9095,172.26.6.88:9095,172.26.6.89:9095";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.34.117:9095,172.26.34.118:9095,172.26.34.119:9095");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServerConfig);
        KafkaProducer producer = new KafkaProducer(properties);
        Map<String, Object> map = (Map<String, Object>) JSON.parseObject(body, Map.class);
        map.put("live_status",live_status);
        producer.send(new ProducerRecord(topicName,JSON.toJSONString(map)));
        //long ts = 1603277440000l;
        producer.close();
    }
}
