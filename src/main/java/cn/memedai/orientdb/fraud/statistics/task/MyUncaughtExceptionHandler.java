package cn.memedai.orientdb.fraud.statistics.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hangyu on 2017/5/22.
 */
public class MyUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyUncaughtExceptionHandler.class);

    public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("caught   " + e);
    }
}
