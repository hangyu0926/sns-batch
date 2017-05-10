package cn.memedai.orientdb.fraud.statistics.main;

import cn.memedai.orientdb.fraud.statistics.utils.ConfigUtils;
import cn.memedai.orientdb.fraud.statistics.utils.DbUtils;
import cn.memedai.orientdb.fraud.statistics.utils.OrientDbUtils;
import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hangyu on 2017/5/10.
 */
public class AddDataImportMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddDataImportMain.class);

    public void getApplyphonetag (List<String> applyNos){
        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        ;
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != applyNos && applyNos.size() > 0) {
            SqlUtils.getApplyphonetag(applyNos, conn, mysqlConn);
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

    public void getOrderphonetag (List<String> orderNoList){
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
            for (String orderNo : orderNoList) {
                    pstmt = conn.prepareStatement("select applyinfo.applyNo as applyNo from (MATCH{class:Order,where: (orderNo = ?)}<-ApplyHasOrder-{as:applyinfo,class:Apply} return applyinfo)");
                    pstmt.setString(1, orderNo);
                    rs = pstmt.executeQuery();
                    while (rs.next()){
                        if (null != rs.getString("applyNo")){
                            applyNosHasOrders.add(rs.getString("applyNo"));
                        }else{
                            orderNos.add(orderNo);
                        }
                    }
                }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (null != pstmt){
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (null != rs){
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
}
