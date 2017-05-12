package cn.memedai.orientdb.fraud.statistics.task;

import cn.memedai.orientdb.fraud.statistics.main.AddDataImportMain;
import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hangyu on 2017/5/12.
 */
public class BasicDataBatchTask implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDataBatchTask.class);

    List<String> applyNos = new ArrayList<String>();
    List<String> orderNos = new ArrayList<String>();

    public void run() {
        SqlUtils.getApplyphonetag(applyNos);
        SqlUtils.getOrderphonetag(orderNos);
    }

    public BasicDataBatchTask() {
        super();
    }

    public List<String> getApplyNos() {
        return applyNos;
    }

    public void setApplyNos(List<String> applyNos) {
        for (String apply : applyNos) {
            this.applyNos.add(apply);
        }
    }

    public List<String> getOrderNos() {
        return orderNos;
    }

    public void setOrderNos(List<String> orderNos) {
        for (String order : orderNos) {
            this.orderNos.add(order);
        }
    }
}
