package cn.memedai.orientdb.fraud.statistics.utils;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by hangyu on 2017/5/15.
 */
public class DateUtils {

    public static String getStartDatetime(String startDatetime, int i) {
        if (StringUtils.isBlank(startDatetime)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date(Calendar.getInstance().getTimeInMillis() - i * 3600 * 24 * 1000);
            return sdf.format(date);
        }
        return startDatetime;
    }

    public static void main(String[] args) {
        System.out.println(getStartDatetime("",1));
    }
}
