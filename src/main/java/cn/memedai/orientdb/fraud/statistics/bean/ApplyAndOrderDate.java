package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;

/**
 * Created by hangyu on 2017/5/18.
 */
public class ApplyAndOrderDate implements Serializable{

    private static final long serialVersionUID = 1768467694348358850L;

    private String no;

    private String createDatetime;

    public ApplyAndOrderDate() {
        super();
    }

    @Override
    public String toString() {
        return "ApplyAndOrderDate{" +
                "no='" + no + '\'' +
                ", createDatetime='" + createDatetime + '\'' +
                '}';
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getCreateDatetime() {
        return createDatetime;
    }

    public void setCreateDatetime(String createDatetime) {
        this.createDatetime = createDatetime;
    }
}
