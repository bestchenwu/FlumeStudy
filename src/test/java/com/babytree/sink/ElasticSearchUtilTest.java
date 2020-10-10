package com.babytree.sink;

import com.babytree.util.ElasticSearchUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

public class ElasticSearchUtilTest {

    @Test
    public void testCreateEsId(){
        Map<String,Object> map = new HashMap<>();
        map.put("discuss_id",13);
        map.put("item_type",2);
        String esId = ElasticSearchUtil.createEsId(map, "discuss_id,item_type");
        Assert.assertEquals("13_2",esId);
    }

    @Test
    public void testCreateEsId2(){
        Map<String,Object> map = new HashMap<>();
        map.put("discuss_id",13);
        String esId = ElasticSearchUtil.createEsId(map, "discuss_id,item_type");
        Assert.assertEquals("13",esId);
    }

    @Test
    public void testCreateEsId3(){
        Map<String,Object> map = new HashMap<>();
        //map.put("discuss_id",13);
        String esId = ElasticSearchUtil.createEsId(map, "discuss_id,item_type");
        Assert.assertEquals("",esId);
    }
}
