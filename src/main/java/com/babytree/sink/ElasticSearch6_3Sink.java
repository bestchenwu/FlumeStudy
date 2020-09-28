package com.babytree.sink;

import com.google.gson.Gson;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;

/**
 * 自定义输出到es6.3的sink
 *
 * @author chenwu on 2020.9.28
 */
public class ElasticSearch6_3Sink extends AbstractSink implements Configurable {

    private String hostNames;
    private String indexName;
    private String clusterName;
    private String idField ;
    private int batchSize = 100;
    static TransportClient transportClient;
    private int retry_times;
    private Gson gson ;

    @Override
    public synchronized void start() {
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        try {
            transportClient = new PreBuiltTransportClient(settings);
            String[] splitArray = hostNames.split(",");
            TransportAddress[] addresses = new TransportAddress[splitArray.length];
            for(int i = 0;i<splitArray.length;i++){
                String[] split = splitArray[i].split(":");
                addresses[i] = new TransportAddress(InetAddress.getByName(split[0]),Integer.parseInt(split[1]));
            }
            for(TransportAddress address : addresses){
                transportClient.addTransportAddress(address);
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        gson = new Gson();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status =  Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try{
            List<Map<String,Object>> datas = new ArrayList<>();
            for(int i = 0;i<batchSize;i++){
                Event event = channel.take();
                byte[] body = event.getBody();
                if(body == null ){
                    status = Status.BACKOFF;
                    break;
                }
                Map<String,Object> map = gson.fromJson(String.valueOf(body),Map.class);
                map.put("cmd","add");
                map.put("id",map.get(idField));
                datas.add(map);
            }
            if(datas.size()>0){
                bulk(indexName,datas);
            }
            transaction.commit();
        }catch(Throwable e){
            transaction.rollback();
            status = Status.BACKOFF;
            if(e instanceof Error){
                throw e;
            }
        }finally {
            if(transaction!=null){
                transaction.close();
            }
        }
        return status;
    }

    private boolean bulk(String indexName, List<Map<String,Object>> datas){
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

        for(Map<String,Object> map : datas){
            UpdateRequest updateRequest = new UpdateRequest().index(indexName)
                    .type("_doc").id(map.get("id").toString()).doc(map).retryOnConflict(retry_times).upsert(map);
            bulkRequestBuilder.add(updateRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        return bulkResponse.hasFailures();
    }

    @Override
    public void configure(Context context) {
        hostNames = context.getString(HOSTNAMES);
        indexName = context.getString(INDEX_NAME);
        clusterName = context.getString(CLUSTER_NAME);
        batchSize = Optional.of(context.getInteger("batchSize")).orElse(100);
        retry_times  = Optional.of(context.getInteger("retry_times")).orElse(3);
        //主键id字段
        idField = context.getString("elasticSearchIds");
    }
}
