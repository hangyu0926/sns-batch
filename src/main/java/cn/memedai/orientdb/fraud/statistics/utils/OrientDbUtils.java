package cn.memedai.orientdb.fraud.statistics.utils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by tizhou on 2017/4/28.
 */
public final class OrientDbUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrientDbUtils.class);

    private OrientDbUtils() {
        throw new RuntimeException("can't instance this object!");
    }

    public static Connection getConnection(String url, String userName, String password) {
        Properties info = new Properties();
        info.put("user", userName);
        info.put("password", password);

        info.put("db.usePool", ConfigUtils.getProperty("orientDb.usePool.isUserFlag")); // USE THE POOL
        info.put("db.pool.min", ConfigUtils.getProperty("orientDb.pool.min"));   // MINIMUM POOL SIZE
        info.put("db.pool.max", ConfigUtils.getProperty("orientDb.pool.max"));  // MAXIMUM POOL SIZE

        OrientJdbcConnection conn = null;
        try {
            conn = (OrientJdbcConnection) DriverManager.getConnection(url, info);
        } catch (SQLException e) {
            LOGGER.error("orientDb getConnection has e is {}", e);
        }

        return conn;
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                LOGGER.info("orientDb connection : " + connection);
            } catch (SQLException e) {
                LOGGER.error("orientDb close has e is {}", e);
            }
        }
    }

    public static void close(ODatabaseDocumentTx tx) {
        if (tx != null) {
            tx.close();
        }
    }
}
