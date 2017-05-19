package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hangyu on 2017/5/18.
 */
public class MemberAndPhoneBean implements Serializable {
    private static final long serialVersionUID = -7423811764733016193L;

    private String memberId;

    private String phones;

    private String dateTime;

    private List<ApplyRelateOrder> onlyAppNos = new ArrayList<ApplyRelateOrder>();

    private List<ApplyRelateOrder> onlyOrderNos = new ArrayList<ApplyRelateOrder>();

    private List<ApplyRelateOrder> applyRelateOrderNos = new ArrayList<ApplyRelateOrder>();

    public MemberAndPhoneBean() {
        super();
    }

    @Override
    public String toString() {
        return "MemberAndPhoneBean{" +
                "memberId='" + memberId + '\'' +
                ", phones='" + phones + '\'' +
                ", dateTime='" + dateTime + '\'' +
                ", onlyAppNos=" + onlyAppNos +
                ", onlyOrderNos=" + onlyOrderNos +
                ", applyRelateOrderNos=" + applyRelateOrderNos +
                '}';
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getPhones() {
        return phones;
    }

    public void setPhones(String phones) {
        this.phones = phones;
    }

    public List<ApplyRelateOrder> getOnlyAppNos() {
        return onlyAppNos;
    }

    public void setOnlyAppNos(List<ApplyRelateOrder> onlyAppNos) {
        if (onlyAppNos != null) {
            for (ApplyRelateOrder str : onlyAppNos) {
                this.onlyAppNos.add(str);
            }
        }
    }

    public List<ApplyRelateOrder> getOnlyOrderNos() {
        return onlyOrderNos;
    }

    public void setOnlyOrderNos(List<ApplyRelateOrder> onlyOrderNos) {
        if (onlyOrderNos != null) {
            for (ApplyRelateOrder str : onlyOrderNos) {
                this.onlyOrderNos.add(str);
            }
        }
    }

    public List<ApplyRelateOrder> getApplyRelateOrderNos() {
        return applyRelateOrderNos;
    }

    public void setApplyRelateOrderNos(List<ApplyRelateOrder> applyRelateOrderNos) {
        if (applyRelateOrderNos != null) {
            for (ApplyRelateOrder str : applyRelateOrderNos) {
                this.applyRelateOrderNos.add(str);
            }
        }
    }
}
