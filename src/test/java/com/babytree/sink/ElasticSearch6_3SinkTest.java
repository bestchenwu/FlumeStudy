package com.babytree.sink;

import com.google.gson.Gson;
import org.junit.Test;
import java.util.Map;

public class ElasticSearch6_3SinkTest {

    @Test
    public void testParse(){
        String str = "{\"mac\":\"test2\",\"recent_3_query\":\"test21,test22\"}";
        Gson gson = new Gson();
        Map map = gson.fromJson(str, Map.class);
        System.out.println(map);
    }
}
