package com.babytree.sink;

import com.alibaba.fastjson.JSON;

import java.util.Map;

public class SingleRowSendTest {

    public static void main(String[] args) {
        String body = "{\"appointment_end_ts\":1603274580000000,\"appointment_start_ts\":1603274580000,\"business\":\"pregnancy\",\"comment_total\":0,\"comment_total_actual\":0,\"comment_total_base\":0,\"comment_user_actual\":0,\"comment_user_base\":0,\"comment_user_total\":0,\"discussion_id\":94754262,\"distribute_ts\":1603274580000,\"during\":1603277440000,\"end_ts\":1603277440000,\"follower_total\":0,\"follower_total_actual\":0,\"follower_total_base\":0,\"item_type\":1,\"live_status\":3,\"name\":\"爱动漫的初冬的雪\",\"online_max_total\":3,\"online_max_total_actual\":3,\"online_max_total_base\":0,\"owner_id\":\"u12400672386\",\"owner_type\":1,\"praise_total\":0,\"praise_total_actual\":0,\"praise_total_base\":0,\"pv_total\":6,\"pv_total_actual\":6,\"pv_total_base\":0,\"question_total\":0,\"question_total_actual\":0,\"question_total_base\":0,\"scence_id\":6326,\"start_ts\":0,\"user_type\":0,\"uv_total\":4,\"uv_total_actual\":4,\"uv_total_base\":0,\"video_type\":6}";
        Map<String, Object> map = (Map<String, Object>)JSON.parseObject(body, Map.class);
        System.out.println(map);
    }
}
