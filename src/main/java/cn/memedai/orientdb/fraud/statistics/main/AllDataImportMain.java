package cn.memedai.orientdb.fraud.statistics.main;

import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hangyu on 2017/4/28.
 */
public class AllDataImportMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(AllDataImportMain.class);
    public static void main(String[] args) {
        LOGGER.info("AllDataImportMain ..............");
        SqlUtils.queryBasicData(null);
    }


}
