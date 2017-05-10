package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.ConstantHelper;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ConfigUtils {

    /**
     * ��־��¼��
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * ����
     */
    private static PropertiesConfiguration PROP = new PropertiesConfiguration();

    static {
        PROP.setEncoding("utf-8");
        PROP.setFileName(ConstantHelper.BUSINESS_CONF_PROPS);
        try {
            PROP.load();
        } catch (Exception e) {
            LOGGER.error("------ ����ҵ�������ļ������쳣 ------{}", e);
            try {
                PROP.load();
            } catch (Exception e1) {
                LOGGER.error("------ �ڶ��γ��Լ���ҵ�������ļ������쳣 ------{}", e1);
            }
        }
    }

    /**
     *
     * ��������: <br>
     * ���ݼ�����ȡ��Դ�ļ��е�ֵ
     *
     * @author Vic Ding
     * @version [�汾��, 2016��1��8��]
     * @param key ��
     * @return ֵ
     */
    public static String getProperty(String key) {
        // ��ȡ
        String value = null;
        if (PROP.containsKey(key)) {
            value = PROP.getString(key);
        } else {
            LOGGER.error("------ û���ҵ���Ӧ��������Ϣ��key:" + key + " ------");
        }

        return value;
    }

    /**
     *
     * ��������: <br>
     * ��ȡ��̬��ʾ��Ϣ
     *
     * @author Vic Ding
     * @version [�汾��, 2016��1��8��]
     * @param key ��
     * @param args ��̬����
     * @return ��̬��ʾ
     */
    public static String getArgs(String key, Object[] args) {
        // ��ȡ
        String value = null;
        if (PROP.containsKey(key)) {
            value = PROP.getString(key);
            // �������
            String.format(value, args);
        } else {
            LOGGER.error("------ û���ҵ���Ӧ��������Ϣ��key:" + key + " ------");
        }

        return value;
    }

}
