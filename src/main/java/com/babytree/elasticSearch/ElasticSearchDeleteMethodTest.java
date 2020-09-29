package com.babytree.elasticSearch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 删除elasticSearch里面的错误数据
 *
 * @author chenwu on 2020.9.29
 */
public class ElasticSearchDeleteMethodTest {

    private RestHighLevelClient restHighLevelClient;
    private String indexName;
    private List<String> deleteIds = new ArrayList<>();

    public void setUp(String hostNames,String indexName){
        restHighLevelClient = buildHighLevelClient(hostNames);
        this.indexName = indexName;
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

    private void deleteById() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for(String id : deleteIds){
            DeleteRequest deleteRequest = new DeleteRequest(indexName,"_doc",id);
            bulkRequest.add(deleteRequest);
        }
        BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, new Header[0]);
        if(bulkItemResponses.hasFailures()){
            System.out.println(bulkItemResponses.buildFailureMessage());
        }
    }

    public void close(){
        if(restHighLevelClient!=null){
            try{
                restHighLevelClient.close();
            }catch(IOException e){
                ExceptionUtils.printRootCauseStackTrace(e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchDeleteMethodTest elasticSearchDeleteMethodTest = new ElasticSearchDeleteMethodTest();
        elasticSearchDeleteMethodTest.setUp(args[0],args[1]);
        if(args.length==3){
            elasticSearchDeleteMethodTest.deleteIds = Arrays.asList(args[2],args[2]+".0");
        }else{
            int fromId = Integer.parseInt(args[2]);
            int endId = Integer.parseInt(args[3]);
            for(int i = fromId;i<=endId;i++){
                elasticSearchDeleteMethodTest.deleteIds.add(String.valueOf(i));
                elasticSearchDeleteMethodTest.deleteIds.add(String.valueOf(i)+".0");
            }
        }
        System.out.println(elasticSearchDeleteMethodTest.deleteIds);
        elasticSearchDeleteMethodTest.deleteById();
        elasticSearchDeleteMethodTest.close();
    }
}
