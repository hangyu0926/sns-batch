package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;

/**
 * Created by hangyu on 2017/5/19.
 */
public class SameIpBean implements Serializable{
    private static final long serialVersionUID = -8449423561443909601L;

    private String ip;

    private int direct;

    public SameIpBean() {
        super();
    }

    @Override
    public String toString() {
        return "SameIpBean{" +
                "ip='" + ip + '\'' +
                ", direct=" + direct +
                '}';
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getDirect() {
        return direct;
    }

    public void setDirect(int direct) {
        this.direct = direct;
    }
}
