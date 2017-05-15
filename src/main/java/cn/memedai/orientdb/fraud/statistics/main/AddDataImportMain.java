package cn.memedai.orientdb.fraud.statistics.main;

import cn.memedai.orientdb.fraud.statistics.task.BasicDataBatchTask;
import cn.memedai.orientdb.fraud.statistics.utils.*;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by hangyu on 2017/5/10.
 */
public class AddDataImportMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddDataImportMain.class);

    public static void main(String[] args) {
        //String dateIn="2017-05-12";
        String date = null;

        if (null != args && args.length > 0){
            String dateIn = args[0];
            date = dateIn.replaceAll("\r|\n", "");
        }else{
            date = DateUtils.getStartDatetime(date,1);
        }

        SqlUtils.queryBasicData(date);
    }

}
