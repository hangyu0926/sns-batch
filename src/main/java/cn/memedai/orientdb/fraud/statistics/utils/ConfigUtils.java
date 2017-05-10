package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.ConstantHelper;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ConfigUtils {

    /**
     * 日志记录器
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * 配置
     */
    private static PropertiesConfiguration PROP = new PropertiesConfiguration();

    static {
        PROP.setEncoding("utf-8");
        PROP.setFileName(ConstantHelper.BUSINESS_CONF_PROPS);
        try {
            PROP.load();
        } catch (Exception e) {
            LOGGER.error("------ 加载业务配置文件出现异常 ------{}", e);
            try {
                PROP.load();
            } catch (Exception e1) {
                LOGGER.error("------ 第二次尝试加载业务配置文件出现异常 ------{}", e1);
            }
        }
    }

    /**
     *
     * 功能描述: <br>
     * 根据键来获取资源文件中的值
     *
     * @author Vic Ding
     * @version [版本号, 2016年1月8日]
     * @param key 键
     * @return 值
     */
    public static String getProperty(String key) {
        // 获取
        String value = null;
        if (PROP.containsKey(key)) {
            value = PROP.getString(key);
        } else {
            LOGGER.error("------ 没有找到对应的配置信息，key:" + key + " ------");
        }

        return value;
    }

    /**
     *
     * 功能描述: <br>
     * 获取动态提示信息
     *
     * @author Vic Ding
     * @version [版本号, 2016年1月8日]
     * @param key 键
     * @param args 动态参数
     * @return 动态提示
     */
    public static String getArgs(String key, Object[] args) {
        // 获取
        String value = null;
        if (PROP.containsKey(key)) {
            value = PROP.getString(key);
            // 填入参数
            String.format(value, args);
        } else {
            LOGGER.error("------ 没有找到对应的配置信息，key:" + key + " ------");
        }

        return value;
    }

}
