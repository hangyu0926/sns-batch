package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by kisho on 2017/4/6.
 */
public final class DbUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");

        } catch (ClassNotFoundException e) {
        }

    }

    private DbUtils() {
        throw new RuntimeException("can't instance this object!");
    }

    public static Connection getConnection(String url, String userName, String password) {
        try {
            Connection connection = DriverManager.getConnection(url, userName, password);
//            LOGGER.info("ooo connection : " + connection);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
//                LOGGER.info("xxx connection : " + connection);
            } catch (SQLException e) {

            }
        }
    }

    public static void close(ODatabaseDocumentTx tx) {
        if (tx != null) {
            tx.close();
        }
    }

}
