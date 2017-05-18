package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hangyu on 2017/5/17.
 */
public class DirectRelationDataBean implements Serializable {

    private static final long serialVersionUID = -2866604099762476529L;

    private HashMap<String, Integer> map;

    private HashMap<String, Integer> map2;

    private long memberId;

    private String phoneNo;

    private String orderNo;

    @Override
    public String toString() {
        return "DirectRelationDataBean{" +
                "map=" + map +
                ", map2=" + map2 +
                ", memberId=" + memberId +
                ", phoneNo='" + phoneNo + '\'' +
                ", orderNo='" + orderNo + '\'' +
                '}';
    }

    public DirectRelationDataBean() {
        super();
    }

    public HashMap<String, Integer> getMap2() {
        return map2;
    }

    public void setMap2(HashMap<String, Integer> map2) {
        this.map2 = map2;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public HashMap<String, Integer> getMap() {
        return map;
    }

    public void setMap(HashMap<String, Integer> map) {
        this.map = map;
    }

    public long getMemberId() {
        return memberId;
    }

    public void setMemberId(long memberId) {
        this.memberId = memberId;
    }

    public String getPhoneNo() {
        return phoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        this.phoneNo = phoneNo;
    }
}
