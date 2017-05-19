package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;

/**
 * Created by hangyu on 2017/5/18.
 */
public class ApplyRelateOrder implements Serializable{

    private static final long serialVersionUID = -330118632104040810L;

    private String apply;

    private String order;

    public ApplyRelateOrder() {
        super();
    }

    @Override
    public String toString() {
        return "ApplyRelateOrder{" +
                "apply='" + apply + '\'' +
                ", order='" + order + '\'' +
                '}';
    }

    public String getApply() {
        return apply;
    }

    public void setApply(String apply) {
        this.apply = apply;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }
}
