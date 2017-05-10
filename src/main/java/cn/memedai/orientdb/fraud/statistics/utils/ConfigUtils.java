package cn.memedai.orientdb.fraud.statistics.utils;

import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * Created by kisho on 2017/4/6.
 */
public final class ConfigUtils {

    private static final Properties PROP = new Properties();

    static {
        try {
            String configPath = System.getenv("config_path");
            if (StringUtils.isBlank(configPath)) {
                configPath = System.getProperty("config_path");
            }
            PROP.load(new FileInputStream(configPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return PROP.getProperty(key);
    }

    public static int getInt(String key) {
        return Integer.parseInt(getProperty(key));
    }

    public static int getPhoneConsumerDoneCount() {
        return getInt("phoneConsumerDoneCount");
    }

    public static int[] getIntArr(String key) {
        String queueConfig = getProperty(key);
        String[] strArr = queueConfig.split("\\|");
        int[] intArr = new int[strArr.length];
        for (int i = 0; i < strArr.length; i++) {
            intArr[i] = Integer.parseInt(strArr[i]);
        }
        return intArr;
    }

    public static String[] getStrArr(String key) {
        return getProperty(key).split("\\|");
    }


    public static String getDefaultLimitCreatedDatetime() {
        String limitCreatedDatetime = ConfigUtils.getProperty("defaultLimitCreatedDatetime");
        if (StringUtils.isBlank(limitCreatedDatetime)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date(Calendar.getInstance().getTimeInMillis() - 3600 * 24 * 1000);
            limitCreatedDatetime = sdf.format(date) + " 00:00:00";
        }
        return limitCreatedDatetime;
    }

}
