package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.entity.IndexData;
import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import cn.memedai.orientdb.fraud.statistics.main.AddDataImportMain;
import cn.memedai.orientdb.fraud.statistics.task.BasicDataBatchTask;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by hangyu on 2017/5/10.
 */
public class SqlUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    public static void getApplyphonetag(List<String> applyNoList) {

        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        ;
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<String> applyNos = new ArrayList<String>();
        try {
            LOGGER.info("applyNoList size is " + applyNoList.size());
            for (String applyNo : applyNoList) {
                pstmt = conn.prepareStatement("select orderinfo.orderNo as orderNo from (MATCH{class:Apply,where: (applyNo = ?)}-ApplyHasOrder->{as:orderinfo,class:Order} return orderinfo)");
                pstmt.setString(1, applyNo);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    //如果此申请单号有订单号则暂不跑，等订单传递来再进行跑批
                } else {
                    applyNos.add(applyNo);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (null != pstmt) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (null != rs) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        if (null != applyNos && applyNos.size() > 0) {
            SqlUtils.getApplyphonetag(applyNos, conn, mysqlConn);
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != mysqlConn) {
            try {
                mysqlConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void getOrderphonetag(List<String> orderNoList) {
        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        ;
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List applyNosHasOrders = new ArrayList<String>();
        List orderNos = new ArrayList<String>();

        try {
            LOGGER.info("orderNoList size is " + orderNoList.size());
            for (String orderNo : orderNoList) {
                pstmt = conn.prepareStatement("select applyinfo.applyNo as applyNo from (MATCH{class:Order,where: (orderNo = ?)}<-ApplyHasOrder-{as:applyinfo,class:Apply} return applyinfo)");
                pstmt.setString(1, orderNo);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    if (null != rs.getString("applyNo")) {
                        applyNosHasOrders.add(rs.getString("applyNo"));
                    }
                } else {
                    orderNos.add(orderNo);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (null != pstmt) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (null != rs) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


        if (null != orderNos && orderNos.size() > 0) {
            SqlUtils.getOrderphonetag(orderNos, conn, mysqlConn);
        }

        if (null != applyNosHasOrders && applyNosHasOrders.size() > 0) {
            SqlUtils.getApplyNosHasOrdersphonetag(applyNosHasOrders, conn, mysqlConn);
        }


        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            LOGGER.info("AddDataImportMain getApplyphonetag   conn.close() end");
        }
        if (null != mysqlConn) {
            try {
                mysqlConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            LOGGER.info("AddDataImportMain getApplyphonetag   mysqlConn.close() end");
        }
    }

    public static void getApplyphonetag(List<String> applyNos, Connection conn, Connection mysqlConn) {
        Map<String, List<IndexData>> resultMap = new HashMap<String, List<IndexData>>();
        List<IndexData> indexDatas = new ArrayList<IndexData>();
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        for (int i = 0; i < applyNos.size(); i++) {
            String applyNo = applyNos.get(i);
            LOGGER.info("applyNo is" + applyNo);
            ResultSet rs = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                sql = "select count(memberHasPhoneCalltoPhone) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "       where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        "       applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                rs = getResultSet(conn, sql, applyNo);

                map = setPhoneTagIndexDatas(rs, indexDatas, list, map);

                //查找出一度联系人的电话号码
                sql = " select applyInfo.applyNo as applyNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                rs = getResultSet(conn, sql, applyNo);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            list = null;
            map = null;
        }

        insertPhonetagIndex(indexDatas, mysqlConn);
        indexDatas.clear();
        indexDatas = null;

        for (int i = 0; i < applyNos.size(); i++) {
            String applyNo = applyNos.get(i);
            ResultSet rs = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from " +
                        "      (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}<-PhoneHasApply-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasDevice->{class: Device, as: device}" +
                        "       <-MemberHasDevice-{as:deviceMember, class: Member} return applyInfo,phone,device,member,deviceMember)";
                rs = getResultSet(conn, sql, applyNo);

                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num");

                //同IP客户个数
                sql = "select count(ipMember) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from " +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}<-PhoneHasApply-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasIp->{class: Ip, as: ip}" +
                        "    <-MemberHasIp-{as:ipMember, class: Member} return applyInfo,phone,ip,member,ipMember)";
                rs = getResultSet(conn, sql, applyNo);

                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num");

                //连接设备的个数
                sql = "select count(device) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "     <-PhoneHasApply-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasDevice->{as:device,class:device}   return applyInfo,phone,member,device)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num");

                //连接不同ip的个数
                sql = "select count(ip) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)} " +
                        "   <-PhoneHasApply-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasIp->{as:ip,class:ip}   return applyInfo,phone,member,ip)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num");

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)} " +
                        "      <-PhoneHasApply-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{class:apply}-ApplyHasStore->{as:storeinfo,class:store}   return applyInfo,phone,member,storeinfo)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num");

                //连接不同申请件数
                sql = "select count(applys) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "        <-PhoneHasApply-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{as:applys,class:apply}   return applyInfo,phone,member,applys)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num");

                //连接不同订单数
                sql = "select count(orders) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "                    <-PhoneHasApply-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasOrder->{as:orders,class:order}   return applyInfo,phone,member,orders)";
                rs = getResultSet(conn, sql, applyNo);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num");

                //联系过件客户个数

                sql = "select applyInfo.applyNo as applyNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Apply, as:applyInfo," +
                        "     where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{class: Member, as: members} RETURN " +
                        "    applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members)";
                rs = getResultSet(conn, sql, applyNo);
                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas, mysqlConn);
        insertMemberIndex(memberIndexDatas, mysqlConn);

        deviceIndexDatas.clear();
        deviceIndexDatas = null;
        memberIndexDatas.clear();
        memberIndexDatas = null;
    }

    public static void getOrderphonetag(List<String> orderNos, Connection conn, Connection mysqlConn) {
        Map<String, List<IndexData>> resultMap = new HashMap<String, List<IndexData>>();
        List<IndexData> indexDatas = new ArrayList<IndexData>();
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        for (int i = 0; i < orderNos.size(); i++) {
            String orderNo = orderNos.get(i);
            LOGGER.info("orderNo is" + orderNo);
            ResultSet rs = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                sql = "select count(memberHasPhoneCalltoPhone) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Order, as:orderInfo," +
                        "           where:(orderNo=?)}<-PhoneHasOrder-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        "            orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                rs = getResultSet(conn, sql, orderNo);

                map = setPhoneTagIndexDatas(rs, indexDatas, list, map);

                //查找出一度联系人的电话号码
                sql = " select orderInfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Order, as:orderInfo, " +
                        "where:(orderNo=?)}<-PhoneHasOrder-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                rs = getResultSet(conn, sql, orderNo);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            list = null;
            map = null;
        }

        insertPhonetagIndex(indexDatas, mysqlConn);
        indexDatas.clear();
        indexDatas = null;

        for (int i = 0; i < orderNos.size(); i++) {
            String orderNo = orderNos.get(i);
            ResultSet rs = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from " +
                        "  (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}<-PhoneHasOrder-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasDevice->{class: Device, as: device}" +
                        "   <-MemberHasDevice-{as:deviceMember, class: Member} return orderInfo,phone,device,member,deviceMember)";
                rs = getResultSet(conn, sql, orderNo);

                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num");

                //同IP客户个数
                sql = "select count(ipMember) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from " +
                        "    (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}<-PhoneHasOrder-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasIp->{class: Ip, as: ip}" +
                        "    <-MemberHasIp-{as:ipMember, class: Member} return orderInfo,phone,ip,member,ipMember)";
                rs = getResultSet(conn, sql, orderNo);

                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num");

                //连接设备的个数
                sql = "select count(device) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasDevice->{as:device,class:device}   return orderInfo,phone,member,device)";
                rs = getResultSet(conn, sql, orderNo);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num");

                //连接不同ip的个数
                sql = "select count(ip) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasIp->{as:ip,class:ip}   return orderInfo,phone,member,ip)";
                rs = getResultSet(conn, sql, orderNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num");

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "       <-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{class:apply}-ApplyHasStore->{as:storeinfo,class:store}   return orderInfo,phone,member,storeinfo)";
                rs = getResultSet(conn, sql, orderNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num");

                //连接不同申请件数
                sql = "select count(applys) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{as:applys,class:apply}   return orderInfo,phone,member,applys)";
                rs = getResultSet(conn, sql, orderNo);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num");

                //连接不同订单数
                sql = "select count(orders) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "  <-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasOrder->{as:orders,class:order}   return orderInfo,phone,member,orders)";
                rs = getResultSet(conn, sql, orderNo);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num");

                //联系过件客户个数
                sql = "select orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Order, as:orderInfo,where: (orderNo = ?)} " +
                        "   <-PhoneHasOrder-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{class: Member, as: members} RETURN " +
                        "   orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members)";
                rs = getResultSet(conn, sql, orderNo);
                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas, mysqlConn);
        insertMemberIndex(memberIndexDatas, mysqlConn);
        deviceIndexDatas.clear();
        deviceIndexDatas = null;
        memberIndexDatas.clear();
        memberIndexDatas = null;
    }

    public static void getApplyNosHasOrdersphonetag(List<String> applyNosHasOrders, Connection conn, Connection mysqlConn) {
        List<IndexData> indexDatas = new ArrayList<IndexData>();
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        int applyNosHasOrdersSize = applyNosHasOrders.size();
        for (int i = 0; i < applyNosHasOrdersSize; i++) {
            String applyNo = applyNosHasOrders.get(i);
            LOGGER.info("applyNosHasOrders is" + applyNo);
            ResultSet rs = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                //一度
                sql = "select count(memberHasPhoneCalltoPhone) as direct,applyInfo.applyNo as applyNo,orderinfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        " where:(applyNo=?)}-ApplyHasOrder->{as:orderinfo, class: Order}<-PhoneHasOrder-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        " applyInfo,orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                rs = getResultSet(conn, sql, applyNo);
                setPhoneTagIndexDatas(rs, indexDatas, list, map);

                //查找出一度联系人的电话号码
                sql = " select applyInfo.applyNo as applyNo,orderinfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "where:(applyNo=?)}-ApplyHasOrder->{as:orderinfo, class: Order}<-PhoneHasOrder-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  applyInfo,orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                rs = getResultSet(conn, sql, applyNo);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            list = null;
            map = null;
        }

        insertPhonetagIndex(indexDatas, mysqlConn);
        indexDatas = null;

        for (int i = 0; i < applyNosHasOrders.size(); i++) {
            String applyNo = applyNosHasOrders.get(i);
            ResultSet rs = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from" +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasDevice->{class: Device, as: device}" +
                        "   <-MemberHasDevice-{as:deviceMember, class: Member} return applyInfo,orderInfo,phone,device,member,deviceMember)";
                rs = getResultSet(conn, sql, applyNo);

                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num");

                //同IP客户个数
                sql = "select count(ipMember) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from" +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone, class: Phone}<-HasPhone-{class: Member, as: member}-MemberHasIp->{class: Ip, as: ip}" +
                        "   <-MemberHasIp-{as:ipMember, class: Member} return applyInfo,orderInfo,phone,ip,member,ipMember)";
                rs = getResultSet(conn, sql, applyNo);

                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num");

                //连接设备的个数
                sql = "select count(device) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasDevice->{as:device,class:device}   return applyInfo,orderInfo,phone,member,device)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num");

                //连接不同ip的个数
                sql = "select count(ip) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasIp->{as:ip,class:ip}   return applyInfo,orderInfo,phone,member,ip)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num");

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{class:apply}-ApplyHasStore->{as:storeinfo,class:store}   return applyInfo,orderInfo,phone,member,storeinfo)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num");

                //连接不同申请件数
                sql = "select count(applys) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasApply->{as:applys,class:apply}   return applyInfo,orderInfo,phone,member,applys)";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num");

                //连接不同订单数
                sql = "select count(orders) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone,class:phone}<-HasPhone-{as:member,class:member}-MemberHasOrder->{as:orders,class:order}   return applyInfo,orderInfo,phone,member,orders) ";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num");

                //联系过件和拒件客户个数
                sql = "select applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Apply, as:applyInfo,\n" +
                        "where:(applyNo=?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:applyPhone, class: Phone}<-HasPhone-{class: Member, as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{class: Member, as: members} RETURN \n" +
                        "applyInfo,orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members) ";
                rs = getResultSet(conn, sql, applyNo);

                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas, mysqlConn);
        insertMemberIndex(memberIndexDatas, mysqlConn);
        deviceIndexDatas.clear();
        deviceIndexDatas = null;
        memberIndexDatas.clear();
        memberIndexDatas = null;
    }

    private static ResultSet getResultSet(Connection conn, String sql, String applyNo) throws Exception {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, applyNo);
        ResultSet rs = pstmt.executeQuery();
        if (pstmt != null) {
            pstmt.close();
        }
        return rs;
    }


    private static List<IndexData> queryIndirect(Connection conn, ResultSet rs, List<IndexData> indexDatas, List<String> list, IndexData firstIndexData) throws Exception {
        Set<String> directSet = new HashSet<String>();
        HashMap<String, String> indirectMap = new HashMap<String, String>();
        String memberHasPhoneCalltoPhone = "";
        String sql = "";
        ResultSet rs1 = null;
        //int j = 0;
        while (rs.next()) {
            memberHasPhoneCalltoPhone = rs.getString("memberHasPhoneCalltoPhone");
            directSet.add(memberHasPhoneCalltoPhone);
            //找一度联系人的一度联系人
            sql = " select memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Phone, as:memberHasPhone, " +
                    "where:(phone=?)}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                    "RETURN  memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
            rs1 = getResultSet(conn, sql, memberHasPhoneCalltoPhone);
            while (rs1.next()) {
                indirectMap.put(rs1.getString("memberHasPhoneCalltoPhone"), rs1.getString("mark"));
            }
        }

        if (null != indirectMap && indirectMap.size() > 0) {
            //判断是否包含一度联系人
            for (String str : directSet) {
                if (indirectMap.containsKey(str)) {
                    indirectMap.remove(str);
                }
            }
            //判断是否包含自身
            if (indirectMap.containsKey(firstIndexData.getMobile())) {
                indirectMap.remove(firstIndexData.getMobile());
            }

            //统计各种标签的二度联系人
            Set<Map.Entry<String, String>> set = indirectMap.entrySet();
            Map<String, Integer> indirectResultMap = new HashMap<String, Integer>();
            for (Map.Entry<String, String> en : set) {
                if (indirectResultMap.containsKey(en.getValue())) {
                    indirectResultMap.put(en.getValue(), indirectResultMap.get(en.getValue()) + 1);
                } else {
                    indirectResultMap.put(en.getValue(), 1);
                }
            }

            //判断该标签是否包含一度数据
            Set<Map.Entry<String, Integer>> indirectResultSet = indirectResultMap.entrySet();
            for (Map.Entry<String, Integer> en : indirectResultSet) {
                if (list.contains(en.getKey())) {
                    for (IndexData indexData : indexDatas) {
                        if (indexData.getIndexName().equals(IndexNameEnum.fromValue(en.getKey()))) {
                            indexData.setIndirect(en.getValue());
                        }
                    }
                } else {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(firstIndexData.getMemberId());
                    indexData.setApplyNo(firstIndexData.getApplyNo());
                    indexData.setOrderNo(firstIndexData.getOrderNo());
                    indexData.setMobile(firstIndexData.getMobile());
                    indexData.setIndirect(en.getValue());
                    indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
                    indexDatas.add(indexData);
                    indexData = null;
                }
            }
        }

        return indexDatas;
    }

    private static List<IndexData> setMemberIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, String indexName) {
        try {
            while (rs.next()) {
                if (0 != rs.getInt("direct")) {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(rs.getInt("memberId"));
                    indexData.setMobile(rs.getString("phone"));
                    if (null != rs.getString("applyNo")) {
                        indexData.setApplyNo(rs.getString("applyNo"));
                    }
                    if (null != rs.getString("orderNo")) {
                        indexData.setOrderNo(rs.getString("orderNo"));
                    }
                    if (null != rs.getString("ip")) {
                        indexData.setIp(rs.getString("ip"));
                    }
                    if (null != rs.getString("deviceId")) {
                        indexData.setDeviceId(rs.getString("deviceId"));
                    }
                    indexData.setDirect(rs.getInt("direct"));
                    indexData.setIndexName(indexName);

                    memberIndexDatas.add(indexData);
                    indexData = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberIndexDatas;
    }

    private static Map<String, Object> setPhoneTagIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, List<String> list, Map<String, Object> map) {
        IndexData firstIndexData = new IndexData();
        try {
            while (rs.next()) {
                if (null != rs.getString("mark")) {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(rs.getInt("memberId"));
                    indexData.setMobile(rs.getString("phone"));
                    if (null != rs.getString("applyNo")) {
                        indexData.setApplyNo(rs.getString("applyNo"));
                    }
                    if (null != rs.getString("orderNo")) {
                        indexData.setOrderNo(rs.getString("orderNo"));
                    }
                    indexData.setDirect(rs.getInt("direct"));
                    //LOGGER.info("mark的值"+rs.getString("mark").toString());
                    //LOGGER.info("转换后mark的值："+IndexNameEnum.fromValue(rs.getString("mark").toString()));
                    indexData.setIndexName(IndexNameEnum.fromValue(rs.getString("mark").toString()));

                    memberIndexDatas.add(indexData);
                    list.add(rs.getString("mark"));
                    if (null != firstIndexData) {
                        firstIndexData = indexData;
                    }

                    indexData = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        map.put("firstIndexData", firstIndexData);
        return map;
    }

    private static List<IndexData> setOrderMemberIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, Connection conn) {
        ResultSet rs1 = null;
        PreparedStatement pstmt = null;

        //一度联系人过件个数
        int contactAccept = 0;
        //一度联系人拒件个数
        int contactRefuse = 0;
        //一度联系人逾期个数
        int contactOverdue = 0;
        //一度联系人黑名单个数
        int contactBlack = 0;

        boolean flag = true;
        try {
            //此时一度联系人可能为多个，统计所有一度联系人符合条件的订单总和数
            IndexData indexData = new IndexData();
            if (rs.next()) {
                do {
                    if (flag) {
                        indexData.setMemberId(rs.getInt("memberId"));
                        indexData.setMobile(rs.getString("phone"));
                        if (null != rs.getString("applyNo")) {
                            indexData.setApplyNo(rs.getString("applyNo"));
                        }
                        if (null != rs.getString("orderNo")) {
                            indexData.setOrderNo(rs.getString("orderNo"));
                        }
                        flag = false;//只需要赋值一次，提高效率
                    }
                    int memberId = rs.getInt("memberIds");
                    pstmt = conn.prepareStatement("select out('MemberHasOrder').amount[0] as amount,out('MemberHasOrder').orderNo[0] as orderNo,out('MemberHasOrder').status[0] as status," +
                            "out('MemberHasOrder').originalStatus[0] as originalStatus from member where memberId = ? order by createDateTime desc limit 1");
                    pstmt.setInt(1, memberId);
                    rs1 = pstmt.executeQuery();
                    if (rs1.next()) {
                        if ("1051".equals(rs1.getString("originalStatus"))) {
                            contactAccept++;
                        }
                        if ("1013".equals(rs1.getString("originalStatus"))) {
                            contactRefuse++;
                        }
                    }

                    pstmt = conn.prepareStatement("select isBlack as isBlack,isOverdue as isOverdue from member where memberId =?");
                    pstmt.setInt(1, memberId);
                    rs1 = pstmt.executeQuery();
                    if (rs1.next()) {
                        if (true == rs1.getBoolean("isOverdue")) {
                            contactOverdue++;
                        }
                        if (true == rs1.getBoolean("isBlack")) {
                            contactBlack++;
                        }
                    }
                }
                while (rs.next());
            }

            if (0 != contactAccept) {
                indexData.setDirect(contactAccept);
                indexData.setIndexName("contact_accept_member_num");

                memberIndexDatas.add(indexData);
            }
            if (0 != contactRefuse) {
                indexData.setDirect(contactRefuse);
                indexData.setIndexName("contact_refuse_member_num");

                memberIndexDatas.add(indexData);
            }

            if (0 != contactOverdue) {
                indexData.setDirect(contactOverdue);
                indexData.setIndexName("contact_overdue_member_num");

                memberIndexDatas.add(indexData);
            }
            if (0 != contactBlack) {
                indexData.setDirect(contactBlack);
                indexData.setIndexName("contact_black_member_num");

                memberIndexDatas.add(indexData);
            }
            indexData = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberIndexDatas;
    }

    private static void insertPhonetagIndex(List<IndexData> indexDatas, Connection mysqlConn) {

        if (null != indexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into phonetag_index (member_id, apply_no, order_no,mobile,index_name,direct,indirect,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,now(),now())");

                for (int i = 0; i < indexDatas.size(); i++) {
                    pstmt.setLong(1, indexDatas.get(i).getMemberId());
                    pstmt.setString(2, indexDatas.get(i).getApplyNo());
                    pstmt.setString(3, indexDatas.get(i).getOrderNo());
                    pstmt.setString(4, indexDatas.get(i).getMobile());
                    pstmt.setString(5, indexDatas.get(i).getIndexName());
                    pstmt.setLong(6, indexDatas.get(i).getDirect());
                    pstmt.setLong(7, indexDatas.get(i).getIndirect());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static void insertDeviceAndIpIndex(List<IndexData> deviceIndexDatas, List<IndexData> ipIndexDatas, Connection mysqlConn) {

        if (null != deviceIndexDatas && null != ipIndexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into device_index (member_id, apply_no, order_no,mobile,deviceId,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,now(),now())");

                for (int i = 0; i < deviceIndexDatas.size(); i++) {
                    pstmt.setLong(1, deviceIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, deviceIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, deviceIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, deviceIndexDatas.get(i).getMobile());
                    pstmt.setString(5, deviceIndexDatas.get(i).getDeviceId());
                    pstmt.setString(6, deviceIndexDatas.get(i).getIndexName());
                    pstmt.setLong(7, deviceIndexDatas.get(i).getDirect());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();

                pstmt = mysqlConn.prepareStatement("insert into ip_index (member_id, apply_no, order_no,mobile,ip,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,now(),now())");

                for (int i = 0; i < ipIndexDatas.size(); i++) {
                    pstmt.setLong(1, ipIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, ipIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, ipIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, ipIndexDatas.get(i).getMobile());
                    pstmt.setString(5, ipIndexDatas.get(i).getIp());
                    pstmt.setString(6, ipIndexDatas.get(i).getIndexName());
                    pstmt.setLong(7, ipIndexDatas.get(i).getDirect());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static void insertMemberIndex(List<IndexData> memberIndexDatas, Connection mysqlConn) {

        if (null != memberIndexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into member_index (member_id, apply_no, order_no,mobile,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,now(),now())");

                for (int i = 0; i < memberIndexDatas.size(); i++) {
                    pstmt.setLong(1, memberIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, memberIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, memberIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, memberIndexDatas.get(i).getMobile());
                    pstmt.setString(5, memberIndexDatas.get(i).getIndexName());
                    pstmt.setLong(6, memberIndexDatas.get(i).getDirect());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void queryBasicData(String date) {
        boolean isAllDataQueryFlag = true;
        if (!StringUtils.isBlank(date)) {
            isAllDataQueryFlag = false;
        }

        if (!delBasicData(date)) {
            LOGGER.error("delBasicData is fail");
            return;
        }

        Connection mysqlBusinesConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbBusinessSourceUrl"),
                ConfigUtils.getProperty("mysqlDbBusinessUserName"), ConfigUtils.getProperty("mysqlDbBusinessUserPassword"));

        ExecutorService es = new ThreadPoolExecutor(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"))
                , Integer.parseInt(ConfigUtils.getProperty("allDataImportMainMaximumPoolSize")),
                Long.parseLong(ConfigUtils.getProperty("allDataImportMainKeepAliveTime")), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainQueueLength"))));

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int applyCount = 0;
        int orderCount = 0;
        try {
            //查询总数
            if (!isAllDataQueryFlag) {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM apply_info where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
            } else {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM apply_info");
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                applyCount = rs.getInt("total");
            }

            //查询总数
            if (!isAllDataQueryFlag) {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM money_box_order where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
            } else {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM money_box_order");
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                orderCount = rs.getInt("total");
            }

            int startIndex = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainStartIndex"));
            int allNum = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"));
            int applimitNum = (applyCount % allNum == 0) ? applyCount / allNum : (applyCount / allNum + 1);
            int orderlimitNum = (orderCount % allNum == 0) ? orderCount / allNum : (orderCount / allNum + 1);
            for (int i = startIndex; i < allNum; i++) {
                List<String> applyNos = new ArrayList<String>();
                List<String> orderNos = new ArrayList<String>();
                if (!isAllDataQueryFlag) {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no FROM apply_info where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? order by id limit ?,?");
                    pstmt.setString(1, date);
                    pstmt.setInt(2, i * applimitNum);
                    pstmt.setInt(3, applimitNum);
                } else {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no FROM apply_info order by id limit ?,?");
                    pstmt.setInt(1, i * applimitNum);
                    pstmt.setInt(2, applimitNum);
                }
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    applyNos.add(rs.getString("apply_no"));
                }
                if (!isAllDataQueryFlag) {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT order_no FROM money_box_order where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? order by id limit ?,?");
                    pstmt.setString(1, date);
                    pstmt.setInt(2, i * orderlimitNum);
                    pstmt.setInt(3, orderlimitNum);
                } else {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT order_no FROM money_box_order order by id limit ?,?");
                    pstmt.setInt(1, i * orderlimitNum);
                    pstmt.setInt(2, orderlimitNum);
                }
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    orderNos.add(rs.getString("order_no"));
                }

                BasicDataBatchTask basicDataBatchTask = new BasicDataBatchTask();
                basicDataBatchTask.setApplyNos(applyNos);
                basicDataBatchTask.setOrderNos(orderNos);
                es.submit(basicDataBatchTask);
                applyNos.clear();
                orderNos.clear();
            }
            LOGGER.info("已经开启所有的子线程");
            es.shutdown();
            LOGGER.info("shutdown()：启动一次顺序关闭，执行以前提交的任务，但不接受新任务。");
            while (true) {
                if (es.isTerminated()) {
                    LOGGER.info("所有的子线程都结束了！");
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (null != pstmt) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (null != rs) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean delBasicData(String date) {
        boolean isAllDataQueryFlag = false;

        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));
        if (StringUtils.isBlank(date)) {
            isAllDataQueryFlag = true;
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//小写的mm表示的是分钟
            try {
                Date dateFormat = sdf.parse(date);
                Calendar calendar = new GregorianCalendar();
                calendar.setTime(dateFormat);
                calendar.add(calendar.DATE, 1);//把日期往后增加一天.整数往后推,负数往前移动
                dateFormat = calendar.getTime();
                date = sdf.format(dateFormat);
            } catch (Exception e) {
                LOGGER.error("date convert fail e is {}" + e);
                return false;
            }
        }

        PreparedStatement pstmt = null;
        try {
            if (!isAllDataQueryFlag) {
                pstmt = mysqlConn.prepareStatement("delete FROM phonetag_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM ip_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM device_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM member_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();
            } else {
                pstmt = mysqlConn.prepareStatement("delete FROM phonetag_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM ip_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM device_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM member_index");
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
