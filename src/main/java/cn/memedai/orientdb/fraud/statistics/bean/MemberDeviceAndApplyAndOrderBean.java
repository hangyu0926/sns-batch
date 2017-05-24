package cn.memedai.orientdb.fraud.statistics.bean;

import java.io.Serializable;

/**
 * Created by hangyu on 2017/5/19.
 */
public class MemberDeviceAndApplyAndOrderBean implements Serializable{
    private static final long serialVersionUID = 7298918223637765403L;

    private int hasDeviceNum;

    private int hasIpNum;

    private int hasMerchantNum;

    private int hasApplNum;

    private int hasOrderNum;

    private int contactAcceptMemberNum;

    private int contactRefuseMemberNum;

    private int contactOverdueMemberNum;

    private int contactBlackMemberNum;

    private int contactAcceptMemberCallLenNum;

    private int contactRefuseMemberCallLenNum;

    private int contactOverdueMemberCallLenNum;

    private int contactBlackMemberCallLenNum;

    public MemberDeviceAndApplyAndOrderBean() {
        super();
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getHasDeviceNum() {
        return hasDeviceNum;
    }

    public void setHasDeviceNum(int hasDeviceNum) {
        this.hasDeviceNum = hasDeviceNum;
    }

    public int getHasIpNum() {
        return hasIpNum;
    }

    public void setHasIpNum(int hasIpNum) {
        this.hasIpNum = hasIpNum;
    }

    public int getHasMerchantNum() {
        return hasMerchantNum;
    }

    public void setHasMerchantNum(int hasMerchantNum) {
        this.hasMerchantNum = hasMerchantNum;
    }

    public int getHasApplNum() {
        return hasApplNum;
    }

    public void setHasApplNum(int hasApplNum) {
        this.hasApplNum = hasApplNum;
    }

    public int getHasOrderNum() {
        return hasOrderNum;
    }

    public void setHasOrderNum(int hasOrderNum) {
        this.hasOrderNum = hasOrderNum;
    }

    public int getContactAcceptMemberNum() {
        return contactAcceptMemberNum;
    }

    public void setContactAcceptMemberNum(int contactAcceptMemberNum) {
        this.contactAcceptMemberNum = contactAcceptMemberNum;
    }

    public int getContactRefuseMemberNum() {
        return contactRefuseMemberNum;
    }

    public void setContactRefuseMemberNum(int contactRefuseMemberNum) {
        this.contactRefuseMemberNum = contactRefuseMemberNum;
    }

    public int getContactOverdueMemberNum() {
        return contactOverdueMemberNum;
    }

    public void setContactOverdueMemberNum(int contactOverdueMemberNum) {
        this.contactOverdueMemberNum = contactOverdueMemberNum;
    }

    public int getContactBlackMemberNum() {
        return contactBlackMemberNum;
    }

    public void setContactBlackMemberNum(int contactBlackMemberNum) {
        this.contactBlackMemberNum = contactBlackMemberNum;
    }

    public int getContactAcceptMemberCallLenNum() {
        return contactAcceptMemberCallLenNum;
    }

    public void setContactAcceptMemberCallLenNum(int contactAcceptMemberCallLenNum) {
        this.contactAcceptMemberCallLenNum = contactAcceptMemberCallLenNum;
    }

    public int getContactRefuseMemberCallLenNum() {
        return contactRefuseMemberCallLenNum;
    }

    public void setContactRefuseMemberCallLenNum(int contactRefuseMemberCallLenNum) {
        this.contactRefuseMemberCallLenNum = contactRefuseMemberCallLenNum;
    }

    public int getContactOverdueMemberCallLenNum() {
        return contactOverdueMemberCallLenNum;
    }

    public void setContactOverdueMemberCallLenNum(int contactOverdueMemberCallLenNum) {
        this.contactOverdueMemberCallLenNum = contactOverdueMemberCallLenNum;
    }

    public int getContactBlackMemberCallLenNum() {
        return contactBlackMemberCallLenNum;
    }

    public void setContactBlackMemberCallLenNum(int contactBlackMemberCallLenNum) {
        this.contactBlackMemberCallLenNum = contactBlackMemberCallLenNum;
    }

    @Override
    public String toString() {
        return "MemberDeviceAndApplyAndOrderBean{" +
                "hasDeviceNum=" + hasDeviceNum +
                ", hasIpNum=" + hasIpNum +
                ", hasMerchantNum=" + hasMerchantNum +
                ", hasApplNum=" + hasApplNum +
                ", hasOrderNum=" + hasOrderNum +
                ", contactAcceptMemberNum=" + contactAcceptMemberNum +
                ", contactRefuseMemberNum=" + contactRefuseMemberNum +
                ", contactOverdueMemberNum=" + contactOverdueMemberNum +
                ", contactBlackMemberNum=" + contactBlackMemberNum +
                ", contactAcceptMemberCallLenNum=" + contactAcceptMemberCallLenNum +
                ", contactRefuseMemberCallLenNum=" + contactRefuseMemberCallLenNum +
                ", contactOverdueMemberCallLenNum=" + contactOverdueMemberCallLenNum +
                ", contactBlackMemberCallLenNum=" + contactBlackMemberCallLenNum +
                '}';
    }
}
