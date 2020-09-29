package com.babytree.producer;

import java.io.Serializable;

/**
 * 直播实时业务
 *
 * @author chenwu on 2020.9.29
 */
public class PubLiveModel implements Serializable {

    private Long discussion_id;
    private Integer video_type;
    private String appointment_start_ts;
    private Integer pv_total;

    public PubLiveModel(){
        super();
    }

    public Long getDiscussion_id() {
        return discussion_id;
    }

    public void setDiscussion_id(Long discussion_id) {
        this.discussion_id = discussion_id;
    }

    public Integer getVideo_type() {
        return video_type;
    }

    public void setVideo_type(Integer video_type) {
        this.video_type = video_type;
    }

    public String getAppointment_start_ts() {
        return appointment_start_ts;
    }

    public void setAppointment_start_ts(String appointment_start_ts) {
        this.appointment_start_ts = appointment_start_ts;
    }

    public Integer getPv_total() {
        return pv_total;
    }

    public void setPv_total(Integer pv_total) {
        this.pv_total = pv_total;
    }
}
