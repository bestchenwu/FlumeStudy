package com.babytree.sink;

import com.google.gson.Gson;
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

    private String hostNames;
    private String indexName;
    private String idField;
    private int batchSize = 100;
    private RestHighLevelClient restHighLevelClient;
    private int retry_times;
    private Gson gson;

    @Override
    public synchronized void start() {
        try {
            String[] splitArray = hostNames.split(",");
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
        gson = new Gson();
        super.start();
    }

    @Override
    public synchronized void stop() {
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
                        map = gson.fromJson(body, Map.class);
                    }catch(Exception e){
                        throw new IllegalArgumentException("body is not a valid Json string,body="+body);
                    }
                    if(map == null){
                        break;
                    }
                    Object id = map.get(idField);
                    if(id==null){
                        System.err.println("id is missing:"+map);
                       continue;
                    }
                    map.put("cmd", "add");
                    map.put("id", id);
                    dataList.add(map);
                }catch(Exception e){
                    ExceptionUtils.printRootCauseStackTrace(e);
                }
            }
            if (dataList.size() > 0) {
                bulk(indexName, dataList);
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

    private boolean bulk(String indexName, List<Map<String, Object>> datas) throws IOException {
        BulkRequest request = new BulkRequest();
        for (Map<String, Object> map : datas) {
            UpdateRequest updateRequest = new UpdateRequest().index(indexName)
                    .type("_doc").id(map.get("id").toString()).doc(map).retryOnConflict(retry_times).upsert(map);
            request.add(updateRequest);
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(request, new Header[0]);
        return bulkResponse.hasFailures();
    }

    @Override
    public void configure(Context context) {
        hostNames = context.getString(HOSTNAMES);
        indexName = context.getString(INDEX_NAME);
        batchSize = Optional.ofNullable(context.getInteger("batchSize")).orElse(2);
        retry_times = Optional.ofNullable(context.getInteger("retry_times")).orElse(3);
        //主键id字段
        idField = context.getString("elasticSearchIds");
    }
}
