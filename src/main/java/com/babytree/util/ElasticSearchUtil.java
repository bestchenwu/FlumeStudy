package com.babytree.util;

import com.babytree.constants.SymbolConstants;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import sun.awt.Symbol;

import java.util.Map;

/**
 * es的工具类
 *
 * @author chenwu on 2020.10.10
 */
public class ElasticSearchUtil {

    /**
     * 根据传入的参数map和idField构建es的文档ID<br/>
     * 如果idField为逗号分隔，则按照逗号分隔的先后顺序依次从map里取值并按照下划线拼接在一块
     *
     * @param map
     * @param idField
     * @return String es的id
     * @author chenwu on 2020.10.10
     */
    public static String createEsId(Map<String,Object> map, String idField){
        if(MapUtils.isEmpty(map) || StringUtils.isBlank(idField)){
            return SymbolConstants.SYMBOL_EMPTY_STRING;
        }
        String[] idFieldArray = idField.split(SymbolConstants.SYMBOL_DH);
        StringBuilder sb = new StringBuilder();
        for(String item:idFieldArray){
            Object object = map.get(item);
            if(object!=null){
                sb.append(object.toString());
                sb.append(SymbolConstants.SYMBOL_XHX);
            }
        }
        String es_id = sb.toString();
        if(es_id.endsWith(SymbolConstants.SYMBOL_XHX)){
            es_id = es_id.substring(0,es_id.length()-1);
        }
        return es_id;
    }
}
