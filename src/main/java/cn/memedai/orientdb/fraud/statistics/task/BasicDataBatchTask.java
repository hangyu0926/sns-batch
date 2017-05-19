package cn.memedai.orientdb.fraud.statistics.task;

import cn.memedai.orientdb.fraud.statistics.bean.ApplyAndOrderDate;
import cn.memedai.orientdb.fraud.statistics.bean.MemberAndPhoneBean;
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

    boolean isAllData = false;

    private List<MemberAndPhoneBean> memberAndPhoneBeanList = new ArrayList<MemberAndPhoneBean>();

    public void run() {
//
        LOGGER.info("memberAndPhoneBeanList {}" + memberAndPhoneBeanList.toString());
        SqlUtils.getBasicData(memberAndPhoneBeanList, isAllData);
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

    public List<MemberAndPhoneBean> getMemberAndPhoneBeanList() {
        return memberAndPhoneBeanList;
    }

    public void setMemberAndPhoneBeanList(List<MemberAndPhoneBean> memberAndPhoneBeanList) {
        if (memberAndPhoneBeanList != null) {
            for (MemberAndPhoneBean memberAndPhoneBean : memberAndPhoneBeanList) {
                this.memberAndPhoneBeanList.add(memberAndPhoneBean);
            }
        }
    }
}
