package cn.memedai.orientdb.fraud.statistics.main;

import cn.memedai.orientdb.fraud.statistics.entity.IndexData;
import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import cn.memedai.orientdb.fraud.statistics.utils.ConfigUtils;
import cn.memedai.orientdb.fraud.statistics.utils.DbUtils;
import cn.memedai.orientdb.fraud.statistics.utils.OrientDbUtils;
import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hangyu on 2017/4/28.
 */
public class AllDataImportMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllDataImportMain.class);

    public static class MyTask implements Runnable {
        List<Integer> memberIds = new ArrayList<Integer>();

        public void run() {
            indexRunBatch(memberIds);
        }

        public MyTask() {
            super();
        }

        public MyTask(List<Integer> memberIds) {
            this.memberIds = memberIds;
        }

        public List<Integer> getMemberIds() {
            return memberIds;
        }

        public void setMemberIds(List<Integer> memberIds) {
            this.memberIds = memberIds;
        }
    }

    public static void indexRunBatch(List<Integer> memberIds) {
        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        ;
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));
        ;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        List<String> applyNos = null;
        List<String> applyNosHasOrders = null;
        List<String> orderNos = null;
        Map<String, String> map = null;
        try {
            for (int memberId : memberIds) {
                applyNos = new ArrayList<String>();
                applyNosHasOrders = new ArrayList<String>();
                orderNos = new ArrayList<String>();
                map = new HashMap<String, String>();

                pstmt = conn.prepareStatement("select applyInfo.applyNo as applyNo from (MATCH{class:Member,where: (memberId = ?)}-HasPhone->{as:phone, class: Phone}-PhoneHasApply->{as:applyInfo,class:Apply} return applyInfo)");
                pstmt.setInt(1, memberId);
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    pstmt = conn.prepareStatement("select orderinfo.orderNo as orderNo from (MATCH{class:Apply,where: (applyNo = ?)}-ApplyHasOrder->{as:orderinfo,class:Order} return orderinfo)");
                    String applyNo = rs.getString("applyNo");
                    pstmt.setString(1, applyNo);
                    rs1 = pstmt.executeQuery();
                    //判断是否存在记录
                    if (rs1.next()) {
                        //有的话 先获取第一条记录
                        do {
                            map.put(rs1.getString("orderNo"), applyNo);
                            applyNosHasOrders.add(applyNo);
                        }
                        while (rs1.next());
                    } else {
                        applyNos.add(rs.getString("applyNo"));
                    }
                }

                pstmt = conn.prepareStatement("select orderinfo.orderNo as orderNo from (MATCH{class:member,where: (memberId = ?)}-MemberHasOrder->{as:orderinfo,class:order} return orderinfo)");
                pstmt.setInt(1, memberId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    //在map中说明此订单有申请
                    if (null != map.get(rs.getString("orderNo"))) {

                    } else {
                        orderNos.add(rs.getString("orderNo"));
                    }
                }

                if (null != applyNos && applyNos.size() > 0) {
                    SqlUtils.getApplyphonetag(applyNos, conn, mysqlConn);
                }

                if (null != orderNos && orderNos.size() > 0) {
                    SqlUtils.getOrderphonetag(orderNos, conn, mysqlConn);
                }

                if (null != applyNosHasOrders && applyNosHasOrders.size() > 0) {
                    SqlUtils.getApplyNosHasOrdersphonetag(applyNosHasOrders, conn, mysqlConn);
                }
                applyNos.clear();
                applyNos = null;
                applyNosHasOrders.clear();
                applyNosHasOrders = null;
                orderNos.clear();
                orderNos = null;
                map = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != rs1) {
                    rs1.close();
                }
                if (null != pstmt) {
                    pstmt.close();
                }
                if (null != conn) {
                    conn.close();
                }
                if (null != mysqlConn) {
                    mysqlConn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {

        ExecutorService es = new ThreadPoolExecutor(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"))
                , Integer.parseInt(ConfigUtils.getProperty("allDataImportMainMaximumPoolSize")),
                Long.parseLong(ConfigUtils.getProperty("allDataImportMainKeepAliveTime")), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainQueueLength"))));

        final Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        MyTask myTask = new MyTask();

        try {
            int startIndex = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainStartIndex"));
            int allNum = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"));
            int limitNum =  Integer.parseInt(ConfigUtils.getProperty("allDataImportMainOpenThreadExportNum"));
            for (int i = startIndex; i < allNum; i++) {
                ArrayList<Integer> memberIds = new ArrayList<Integer>();
                try {
                    pstmt = conn.prepareStatement("select memberId as memberId from member where out('HasPhone').size() > 0  skip ? limit " + limitNum);
                    pstmt.setInt(1, i * limitNum);
                    rs = pstmt.executeQuery();

                    while (rs.next()) {
                        memberIds.add(rs.getInt("memberId"));
                    }
                } catch (Exception e) {
                    LOGGER.error("prepareStatement have exception is {}, i is {}", e, i);
                    e.printStackTrace();
                    continue;
                } finally {
                    try {
                        rs.close();
                        pstmt.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                myTask.setMemberIds(memberIds);
                es.submit(myTask);
            }
            LOGGER.info("已经开启所有的子线程");
            es.shutdown();
            LOGGER.info("shutdown()：启动一次顺序关闭，执行以前提交的任务，但不接受新任务。");
            while (true) {
                if (es.isTerminated()) {
                    LOGGER.info("所有的子线程都结束了！");
                    break;
                }
                Thread.sleep(1000);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            LOGGER.info("主线程结束");
        }
        LOGGER.info("main is end");
    }











}
