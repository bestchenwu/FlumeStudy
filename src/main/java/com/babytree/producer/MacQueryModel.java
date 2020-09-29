package com.babytree.producer;

import java.io.Serializable;

/**
 * 构建mac query model
 *
 * @author chenwu on 2020.9.29
 */
public class MacQueryModel implements Serializable {

    private String mac;
    private String recent_3_query;

    public MacQueryModel(){
        super();
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getRecent_3_query() {
        return recent_3_query;
    }

    public void setRecent_3_query(String recent_3_query) {
        this.recent_3_query = recent_3_query;
    }

}
