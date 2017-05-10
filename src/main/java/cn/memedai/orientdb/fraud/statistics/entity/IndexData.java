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
    private Date createTime;
    //更新日期
    private Date updateTime;
    //设备id
    private String deviceId;
    //ip
    private String ip;

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

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
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
}
