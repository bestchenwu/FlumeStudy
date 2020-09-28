package com.babytree.sink;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.junit.Test;
import java.util.Map;


import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;


public class ElasticSearch6_3SinkTest {

    @Test
    public void testParse(){
//        String str = "{\"mac\":\"test2\",\"recent_elasticSearchIds3_query\":\"test21,test22\"}";
//        Gson gson = new Gson();
//        Map map = gson.fromJson(str, Map.class);
//        System.out.println(map);
        ElasticSearch6_3Sink elasticSearch6_3Sink = new ElasticSearch6_3Sink();
        Context context = new Context();
        context.put(CLUSTER_NAME,"elasticsearch");
        context.put(INDEX_NAME,"users");
        context.put(HOSTNAMES,"127.0.0.1:9200");
        context.put("elasticSearchIds","mac");
        elasticSearch6_3Sink.configure(context);
        elasticSearch6_3Sink.start();
        Sink.Status process = null;
        try {
            process = elasticSearch6_3Sink.process();
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }
        System.out.println("process:"+process);
    }
}
