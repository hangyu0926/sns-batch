package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;

/**
 * Created by hangyu on 2017/5/19.
 */
public class SameDeviceBean implements Serializable{


    private static final long serialVersionUID = 6606023256843813046L;

    private String deviceId;

    private int direct;

    public SameDeviceBean() {
        super();
    }

    @Override
    public String toString() {
        return "SameDeviceBean{" +
                "deviceId='" + deviceId + '\'' +
                ", direct=" + direct +
                '}';
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public int getDirect() {
        return direct;
    }

    public void setDirect(int direct) {
        this.direct = direct;
    }
}
