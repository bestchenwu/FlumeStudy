package com.babytree.sink;

import com.alibaba.fastjson.JSON;
import com.babytree.util.ElasticSearchUtil;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.*;

/**
 * 自定义输出到es6.3的sink(批量导入)
 *
 * @author chenwu on 2020.9.28
 */
public class ElasticSearch6_3MultiSink extends AbstractSink implements Configurable {

    private String hostNames1;
    private String hostNames2;
    private String indexName;
    private String idField;
    private int batchSize = 100;
    private RestHighLevelClient restHighLevelClient1;
    private RestHighLevelClient restHighLevelClient2;
    private int retry_times;
    private Gson gson;
    private long esWriteTimeout = 30l;

    @Override
    public synchronized void start() {
        restHighLevelClient1 = buildHighLevelClient(hostNames1);
        restHighLevelClient2 =  buildHighLevelClient(hostNames2);
        gson = new Gson();
        super.start();
    }

    private RestHighLevelClient buildHighLevelClient(String hostName){
        if(StringUtils.isBlank(hostName)){
            return null;
        }
        RestHighLevelClient restHighLevelClient = null;
        try {
            String[] splitArray = hostName.split(",");
            List<HttpHost> hostsAndPorts = new ArrayList<>();
            for (int i = 0; i < splitArray.length; i++) {
                String[] ipPortArray = splitArray[i].split(":");
                HttpHost httpHost = new HttpHost(ipPortArray[0], Integer.parseInt(ipPortArray[1]));
                hostsAndPorts.add(httpHost);
            }
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(hostsAndPorts.toArray(new HttpHost[0])));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return restHighLevelClient;
    }

    @Override
    public synchronized void stop() {
        if(restHighLevelClient1!=null){
            try{
                restHighLevelClient1.close();
            }catch(IOException e){
                ExceptionUtils.printRootCauseStackTrace(e);
            }
        }
        if(restHighLevelClient2!=null){
            try{
                restHighLevelClient2.close();
            }catch(IOException e){
                ExceptionUtils.printRootCauseStackTrace(e);
            }
        }
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        List<Map<String, Object>> dataList = new ArrayList<>();
        try {
            transaction.begin();

            for(int i = 0;i<batchSize;i++){
                Event event = channel.take();
                if(event==null){
                    break;
                }
                String body = new String(event.getBody());
                try{
                    Map<String, Object> map = null;
                    try{
                        map = (Map<String, Object>)JSON.parseObject(body, Map.class);
                    }catch(Exception e){
                        throw new IllegalArgumentException("body is not a valid Json string,body="+body);
                    }
                    if(map == null){
                        break;
                    }
                    String es_id = ElasticSearchUtil.createEsId(map,idField);
                    if(StringUtils.isBlank(es_id)){
                        System.err.println("id is missing:"+map);
                       continue;
                    }else{
                        map.put("cmd", "add");
                        map.put("id", es_id);
                        dataList.add(map);
                    }
                }catch(Exception e){
                    ExceptionUtils.printRootCauseStackTrace(e);
                }
            }
            if (dataList.size() > 0) {
                bulk(restHighLevelClient1, dataList);
                bulk(restHighLevelClient2, dataList);
                dataList.clear();
            }
            transaction.commit();
        } catch (Throwable e) {
            transaction.rollback();
            status = Status.BACKOFF;
            ExceptionUtils.printRootCauseStackTrace(e);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return status;
    }

    private boolean bulk(RestHighLevelClient restHighLevelClient, List<Map<String, Object>> datas) throws IOException {
        if(restHighLevelClient==null){
            return false;
        }
        BulkRequest request = new BulkRequest();
        for (Map<String, Object> map : datas) {
            if(((String)map.get("id")).equals("11111_1")){
                System.out.println("map:"+map);
            }
            UpdateRequest updateRequest = new UpdateRequest().index(indexName)
                    .type("_doc").id((String)map.get("id")).doc(map).retryOnConflict(retry_times).upsert(map);
            request.add(updateRequest);
        }
        request.timeout(TimeValue.timeValueSeconds(esWriteTimeout));
        BulkResponse bulkResponse = restHighLevelClient.bulk(request, new Header[0]);
        return bulkResponse.hasFailures();
    }

    @Override
    public void configure(Context context) {
        hostNames1 = Optional.ofNullable(context.getString(HOSTNAMES+"1")).orElse("");
        hostNames2 = Optional.ofNullable(context.getString(HOSTNAMES+"2")).orElse("");
        indexName = context.getString(INDEX_NAME);
        batchSize = Optional.ofNullable(context.getInteger("batchSize")).orElse(2);
        retry_times = Optional.ofNullable(context.getInteger("retry_times")).orElse(3);
        //主键id字段
        idField = context.getString("elasticSearchIds");
        //写超时时间
        esWriteTimeout = Optional.ofNullable(context.getLong("write_time_out")).orElse(30l);
    }
}
