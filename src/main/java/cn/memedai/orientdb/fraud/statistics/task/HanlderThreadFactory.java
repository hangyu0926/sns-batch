package cn.memedai.orientdb.fraud.statistics.task;

import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

/**
 * Created by hangyu on 2017/5/22.
 */
public class HanlderThreadFactory implements ThreadFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HanlderThreadFactory.class);

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setUncaughtExceptionHandler(new MyUncaughtExceptionHandler());//设定线程工厂的异常处理器
        LOGGER.info("eh=" + t.getUncaughtExceptionHandler());
        return t;
    }
}
