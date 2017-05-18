package cn.memedai.orientdb.fraud.statistics.task;

import cn.memedai.orientdb.fraud.statistics.bean.ApplyAndOrderDate;
import cn.memedai.orientdb.fraud.statistics.main.AddDataImportMain;
import cn.memedai.orientdb.fraud.statistics.utils.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hangyu on 2017/5/12.
 */
public class BasicDataBatchTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDataBatchTask.class);

    List<ApplyAndOrderDate> applyNos = new ArrayList<ApplyAndOrderDate>();
    List<ApplyAndOrderDate> orderNos = new ArrayList<ApplyAndOrderDate>();
    boolean isAllData = false;

    public void run() {
        SqlUtils.getApplyphonetag(applyNos, isAllData);
        SqlUtils.getOrderphonetag(orderNos, isAllData);
    }

    public BasicDataBatchTask() {
        super();
    }

    public boolean isAllData() {
        return isAllData;
    }

    public void setAllData(boolean allData) {
        isAllData = allData;
    }

    public List<ApplyAndOrderDate> getApplyNos() {
        return applyNos;
    }

    public void setApplyNos(List<ApplyAndOrderDate> applyNos) {
        for (ApplyAndOrderDate applyAndOrderDate : applyNos) {
            this.applyNos.add(applyAndOrderDate);
        }
    }

    public List<ApplyAndOrderDate> getOrderNos() {
        return orderNos;
    }

    public void setOrderNos(List<ApplyAndOrderDate> orderNos) {
        for (ApplyAndOrderDate applyAndOrderDate : orderNos) {
            this.orderNos.add(applyAndOrderDate);
        }
    }
}
