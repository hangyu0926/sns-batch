package cn.memedai.orientdb.fraud.statistics.main;

import cn.memedai.orientdb.fraud.statistics.entity.IndexData;
import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import cn.memedai.orientdb.fraud.statistics.task.BasicDataBatchTask;
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
    public static void main(String[] args) {
        SqlUtils.queryBasicData(null);
    }


}
