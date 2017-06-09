package cn.memedai.orientdb.fraud.statistics.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by hangyu on 2017/4/28.
 */
public class IndexData implements Serializable{

    //会员编号
    private long memberId;
    //手机号码
    private String mobile;
    //申请书编号
    private String applyNo;
    //订单编号
    private String orderNo;
    //指标类别
    private String item;
    //指标名称
    private String indexName;
    //一度数目
    private long direct;
    //二度数目
    private long indirect;
    //创建日期
    private String createTime;
    //更新日期
    private String updateTime;
    //设备id
    private String deviceId;
    //ip
    private String ip;

    private int applyStatus;

    private int orderStatus;

    public long getMemberId() {
        return memberId;
    }

    public void setMemberId(long memberId) {
        this.memberId = memberId;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getApplyNo() {
        return applyNo;
    }

    public void setApplyNo(String applyNo) {
        this.applyNo = applyNo;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public long getDirect() {
        return direct;
    }

    public void setDirect(long direct) {
        this.direct = direct;
    }

    public long getIndirect() {
        return indirect;
    }

    public void setIndirect(long indirect) {
        this.indirect = indirect;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getApplyStatus() {
        return applyStatus;
    }

    public void setApplyStatus(int applyStatus) {
        this.applyStatus = applyStatus;
    }

    public int getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(int orderStatus) {
        this.orderStatus = orderStatus;
    }
}
