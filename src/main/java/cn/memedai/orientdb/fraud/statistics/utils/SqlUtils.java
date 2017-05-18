package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.ConstantHelper;
import cn.memedai.orientdb.fraud.statistics.bean.ApplyAndOrderDate;
import cn.memedai.orientdb.fraud.statistics.bean.DirectRelationDataBean;
import cn.memedai.orientdb.fraud.statistics.entity.IndexData;
import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import cn.memedai.orientdb.fraud.statistics.task.BasicDataBatchTask;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by hangyu on 2017/5/10.
 */
public class SqlUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    private static ODatabaseDocumentTx getODataBaseDocumentTx() {
        ODatabaseDocumentTx tx = new ODatabaseDocumentTx(ConfigUtils.getProperty("orientDbUrl")).open(ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        return tx;
    }

    /**
     * 根据申请查询一度联系人
     *
     * @param no
     * @param tx
     */
    private static DirectRelationDataBean queryDirectRelationDataByNo(String no, ODatabaseDocumentTx tx, String sql) {
        DirectRelationDataBean directRelationDataBean = new DirectRelationDataBean();
        OResultSet memberRelatedPhone = tx.command(new OCommandSQL(sql)).execute(new Object[]{no});
        if (memberRelatedPhone != null && !memberRelatedPhone.isEmpty()) {
            ODocument memberRelatedPhoneDocument = (ODocument) memberRelatedPhone.get(0);
            OTrackedList memberRelatedPhoneNoList = memberRelatedPhoneDocument.field("MemberRelatedPhone");
            OTrackedList memberRelatedOrderList = memberRelatedPhoneDocument.field("RelatedOrderNo");
            String memberRelatedOrder = null;
            if (memberRelatedOrderList.size() > 0) {
                memberRelatedOrder = memberRelatedOrderList.get(0).toString();
            }
            if (memberRelatedPhoneNoList != null && !memberRelatedPhoneNoList.isEmpty()) {
                int size = memberRelatedPhoneNoList.size();
                for (int i = 0; i < size; i++) {
                    HashMap<String, Integer> map = new HashMap<String, Integer>();
                    HashMap<String, Integer> map2 = new HashMap<String, Integer>();
                    String memberRelatedPhoneNo = memberRelatedPhoneNoList.get(i).toString();
                    long memberId = queryDirectRelationDataByPhoneNo(memberRelatedPhoneNo, tx, map, map2, memberRelatedPhoneNo);
                    directRelationDataBean.setMap(map);
                    directRelationDataBean.setMap2(map2);
                    directRelationDataBean.setMemberId(memberId);
                    directRelationDataBean.setPhoneNo(memberRelatedPhoneNo);
                    directRelationDataBean.setOrderNo(memberRelatedOrder);
                }
            }
            if (memberRelatedPhoneNoList != null) {
                memberRelatedPhoneNoList.clear();
                memberRelatedPhoneNoList = null;
            }
        }

        if (memberRelatedPhone != null) {
            memberRelatedPhone.clear();
            memberRelatedPhone = null;
        }

        return directRelationDataBean;
    }


    /**
     * 根据一度联系手机号查询二度联系手机号基础信息
     *
     * @param memberRelatedPhoneNos
     * @param tx
     */
//    private static DirectRelationDataBean queryIndirectRelationDataByPhoneNo(List<String> memberRelatedPhoneNos, ODatabaseDocumentTx tx, String originPhone) {
//        DirectRelationDataBean directRelationDataBean = null;
//        if (memberRelatedPhoneNos != null && !memberRelatedPhoneNos.isEmpty()) {
//            directRelationDataBean = new DirectRelationDataBean();
//            int size = memberRelatedPhoneNos.size();
//            HashMap<String, Integer> map = new HashMap<String, Integer>();
//            for (int i = 0; i < size; i++) {
//                queryDirectRelationDataByPhoneNo(memberRelatedPhoneNos.get(i), tx, true, map, memberRelatedPhoneNos, originPhone);
//            }
//            directRelationDataBean.setMap(map);
//        }
//        return directRelationDataBean;
//    }

    /**
     * 校验phone合法性
     * @param phone
     * @return
     */
     private static Boolean checkPhone(String phone){
         if (StringUtils.isBlank(phone)){
             LOGGER.info("checkPhone invalid phone is {}", phone);
             return false;
         }

         if (phone.length() >= 2){
             if (ConstantHelper.BUSINESS_PHONE_1.equals(phone.substring(0,1))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
             if (ConstantHelper.BUSINESS_PHONE_2.equals(phone.substring(0,1))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
         }

         if (phone.length() >= 3){
             if (ConstantHelper.BUSINESS_PHONE_3.equals(phone.substring(0,2))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
             if (ConstantHelper.BUSINESS_PHONE_4.equals(phone.substring(0,2))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
         }

         if (phone.length() >= 5){
             if (ConstantHelper.BUSINESS_PHONE_5.equals(phone.substring(0,4))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
             if (ConstantHelper.BUSINESS_PHONE_6.equals(phone.substring(0,4))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
             if (ConstantHelper.BUSINESS_PHONE_7.equals(phone.substring(0,4))){
                 LOGGER.info("checkPhone invalid phone is {}", phone);
                 return false;
             }
         }

         if (phone.length() < ConstantHelper.BUSINESS_PHONE_LENGTH){
             LOGGER.info("checkPhone invalid phone is {}", phone);
             return false;
         }

         if (ConstantHelper.BUSINESS_PHONE_7.equals(phone)){
             LOGGER.info("checkPhone invalid phone is {}", phone);
             return false;
         }

         return true;
     }


    /**
     * @param memberRelatedPhoneNo
     * @param tx
     * @param map
     * @return
     */
    private static long queryDirectRelationDataByPhoneNo(String memberRelatedPhoneNo, ODatabaseDocumentTx tx, Map<String, Integer> map, Map<String, Integer> map2, String originPhone) {
        OResultSet phoneInfos = tx.command(new OCommandSQL("select @rid as phoneRid0, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone = ?")).execute(new Object[]{memberRelatedPhoneNo});
        ODocument phoneInfo = ((ODocument) phoneInfos.get(0));
        ODocument phoneRecord0 = phoneInfo.field("phoneRid0");
        ORecordLazyList members0 = phoneInfo.field("members0");
        long memberId = ((ODocument) members0.get(0)).field("memberId");
        ORecordLazyList ocrs = phoneInfo.field("callTos");
        Map<String, String> tempMap = new HashMap<String, String>();
        List<String> directPhones = new ArrayList<String>();

        if (ocrs != null && !ocrs.isEmpty()) {
            int ocrSize = ocrs.size();
            LOGGER.info("queryDirectRelationDataByPhoneNo ocrSize is {}, originPhone is {}", ocrSize, originPhone);
            for (int j = 0; j < ocrSize; j++) {
                ODocument ocr = (ODocument) ocrs.get(j);
                ODocument tempPhoneRecordIn1 = ocr.field("in");//callTo边
                ODocument tempPhoneRecordOut1 = ocr.field("out");
                //设置一级联系人的record
                ODocument phoneRecord1 = getRid(tempPhoneRecordIn1).equals(getRid(phoneRecord0)) ? tempPhoneRecordOut1 : tempPhoneRecordIn1;//phone点

                String phone = phoneRecord1.field("phone").toString();

                //对一度联系人phone做下校验
                if (directPhones.contains(phone) || originPhone.equals(phone)) {
                    continue;
                }
                directPhones.add(phone);

                //查询二度开始
                if (checkPhone(phone)) {
                    ORidBag inCallTo = phoneRecord1.field("in_CallTo");
                    if (null != inCallTo && !inCallTo.isEmpty()) {
                        Iterator<OIdentifiable> it = inCallTo.iterator();
                        while (it.hasNext()) {
                            OIdentifiable t = it.next();
                            ODocument inphone = (ODocument) t;
                            ODocument phone1 = inphone.field("out");
                            String indirectphone = phone1.field("phone");
                            if (!originPhone.equals(indirectphone)) {
                                ORidBag outHasPhoneMark = phone1.field("out_HasPhoneMark");
                                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                                    Iterator<OIdentifiable> it1 = outHasPhoneMark.iterator();
                                    while (it1.hasNext()) {
                                        OIdentifiable t1 = it1.next();
                                        ODocument phoneMark = (ODocument) t1;
                                        ODocument phoneMark1 = phoneMark.field("in");
                                        String mark = phoneMark1.field("mark");
                                        tempMap.put(indirectphone, mark);
                                    }
                                }
                            }
                        }
                    }

                    ORidBag outCallTo = phoneRecord1.field("out_CallTo");
                    if (null != outCallTo && !outCallTo.isEmpty()) {
                        Iterator<OIdentifiable> it = outCallTo.iterator();
                        while (it.hasNext()) {
                            OIdentifiable t = it.next();
                            ODocument outphone = (ODocument) t;
                            ODocument phone1 = outphone.field("in");
                            String indirectphone = phone1.field("phone");
                            if (!originPhone.equals(indirectphone)) {
                                ORidBag outHasPhoneMark = phone1.field("out_HasPhoneMark");
                                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                                    Iterator<OIdentifiable> it1 = outHasPhoneMark.iterator();
                                    while (it1.hasNext()) {
                                        OIdentifiable t1 = it1.next();
                                        ODocument phoneMark = (ODocument) t1;
                                        ODocument phoneMark1 = phoneMark.field("in");
                                        String mark = phoneMark1.field("mark");
                                        tempMap.put(indirectphone, mark);
                                    }
                                }
                            }
                        }
                    }
                }
                //查询二度结束

                ORidBag outHasPhoneMark = phoneRecord1.field("out_HasPhoneMark");//HasPhoneMark边

                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                    Iterator<OIdentifiable> it = outHasPhoneMark.iterator();
                    while (it.hasNext()) {
                        OIdentifiable t = it.next();
                        ODocument phoneMark = (ODocument) t;
                        ODocument phoneMark1 = phoneMark.field("in");
                        String mark = phoneMark1.field("mark");
                        if (map.containsKey(mark)) {
                            Integer count = map.get(mark) + 1;
                            map.put(mark, count);
                        } else {
                            map.put(mark, 1);
                        }
                    }
                }
            }
        }


        //过滤掉二度联系人中的一度联系人
        for (String str : directPhones) {
            if (tempMap.containsKey(str)) {
                tempMap.remove(str);
            }
        }

        //将tempMap改造成map2
        //判断该标签是否包含一度数据
        Set<Map.Entry<String, String>> tempSet = tempMap.entrySet();
        for (Map.Entry<String, String> en : tempSet) {
            String mark = en.getValue();
            if (map2.containsKey(mark)) {
                Integer count = map2.get(mark) + 1;
                map2.put(mark, count);
            } else {
                map2.put(mark, 1);
            }
        }

        if (directPhones != null) {
            directPhones.clear();
            directPhones = null;
        }

        if (tempMap != null) {
            tempMap.clear();
            tempMap = null;
        }

        if (tempSet != null) {
            tempSet.clear();
            tempSet = null;
        }
        if (ocrs != null) {
            ocrs.clear();
            ocrs = null;
        }

        return memberId;
    }

    private static void dealAllBasicDataByApplyList(List<ApplyAndOrderDate> applyNos, ODatabaseDocumentTx tx) {
        if (applyNos != null && !applyNos.isEmpty()) {
            int size = applyNos.size();
            LOGGER.info("dealAllBasicDataByApplyList size {}", size);
            for (int i = 0; i < size; i++) {
                if (i % 1000 == 0) {
                    LOGGER.info("dealAllBasicDataByApplyList i is {}", i);
                }
                dealBasicDataByApplyNo(applyNos.get(i).getNo(), applyNos.get(i).getCreateDatetime(), tx);
            }
        }
    }

    private static void dealAllBasicDataByOrderList(List<ApplyAndOrderDate> orderNos, ODatabaseDocumentTx tx) {
        if (orderNos != null && !orderNos.isEmpty()) {
            int size = orderNos.size();
            LOGGER.info("dealAllBasicDataByOrderList size {}", size);
            for (int i = 0; i < size; i++) {
                dealBasicDataByOrderNo(orderNos.get(i).getNo(), orderNos.get(i).getCreateDatetime(), tx);
            }
        }
    }

    private static void dealBasicDataByApplyNo(String applyNo, String createdDatetime, ODatabaseDocumentTx tx) {
        String sql = "select in('PhoneHasApply').phone as MemberRelatedPhone, out('ApplyHasOrder').orderNo as RelatedOrderNo from Apply where applyNo = ?";
        DirectRelationDataBean directRelationDataBean = queryDirectRelationDataByNo(applyNo, tx, sql);
        LOGGER.info("directRelationDataBean is " + directRelationDataBean.toString());

        List<IndexData> indexDatas = new ArrayList<IndexData>();
        Map<String, Integer> directResultMap = directRelationDataBean.getMap();
        Set<Map.Entry<String, Integer>> directSet = directResultMap.entrySet();
        List<String> directMarks = new ArrayList<String>();
        for (Map.Entry<String, Integer> en : directSet) {
            IndexData indexData = new IndexData();
            indexData.setMemberId(directRelationDataBean.getMemberId());
            indexData.setApplyNo(applyNo);
            indexData.setOrderNo(directRelationDataBean.getOrderNo());
            indexData.setMobile(directRelationDataBean.getPhoneNo());
            indexData.setDirect(en.getValue());
            indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
            indexData.setCreateTime(createdDatetime);
            indexDatas.add(indexData);
            directMarks.add(en.getKey());
        }


        Map<String, Integer> indirectResultMap = directRelationDataBean.getMap2();
        //判断该标签是否包含一度数据
        Set<Map.Entry<String, Integer>> indirectResultSet = indirectResultMap.entrySet();
        for (Map.Entry<String, Integer> en : indirectResultSet) {
            if (directMarks.contains(en.getKey())) {
                for (IndexData indexData : indexDatas) {
                    if (indexData.getIndexName().equals(IndexNameEnum.fromValue(en.getKey()))) {
                        indexData.setIndirect(en.getValue());
                    }
                }
            } else {
                IndexData indexData = new IndexData();
                indexData.setMemberId(directRelationDataBean.getMemberId());
                indexData.setApplyNo(applyNo);
                indexData.setOrderNo(directRelationDataBean.getOrderNo());
                indexData.setMobile(directRelationDataBean.getPhoneNo());
                indexData.setIndirect(en.getValue());
                indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
                indexData.setCreateTime(createdDatetime);
                indexDatas.add(indexData);
            }
        }
        LOGGER.info("dealBasicDataByApplyNo insertPhonetagIndex applyNo is {}", applyNo);
        insertPhonetagIndex(indexDatas);
        if (indexDatas != null) {
            indexDatas.clear();
            indexDatas = null;
        }

        if (directMarks != null) {
            directMarks.clear();
            directMarks = null;
        }

        if (indirectResultMap != null) {
            indirectResultMap.clear();
            indirectResultMap = null;
        }

        if (directResultMap != null) {
            directResultMap.clear();
            directResultMap = null;
        }
        if (directSet != null) {
            directSet.clear();
            directSet = null;
        }
        if (indirectResultSet != null) {
            indirectResultSet.clear();
            indirectResultSet = null;
        }

    }

    private static void dealBasicDataByOrderNo(String orderNo, String createDatetime, ODatabaseDocumentTx tx) {
        String sql = "select in('PhoneHasOrder').phone as MemberRelatedPhone from Order where orderNo = ?";
        DirectRelationDataBean directRelationDataBean = queryDirectRelationDataByNo(orderNo, tx, sql);

        List<IndexData> indexDatas = new ArrayList<IndexData>();
        Map<String, Integer> directResultMap = directRelationDataBean.getMap();
        Set<Map.Entry<String, Integer>> directSet = directResultMap.entrySet();
        List<String> directMarks = new ArrayList<String>();
        for (Map.Entry<String, Integer> en : directSet) {
            IndexData indexData = new IndexData();
            indexData.setMemberId(directRelationDataBean.getMemberId());
            indexData.setOrderNo(orderNo);
            indexData.setMobile(directRelationDataBean.getPhoneNo());
            indexData.setDirect(en.getValue());
            indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
            indexData.setCreateTime(createDatetime);
            indexDatas.add(indexData);
            directMarks.add(en.getKey());
        }

       /* DirectRelationDataBean directRelationDataBean2 = null;
        ArrayList<String> phoneList = directRelationDataBean.getList();
        if (phoneList != null && !phoneList.isEmpty()) {
            directRelationDataBean2 = queryIndirectRelationDataByPhoneNo(phoneList, tx, directRelationDataBean.getPhoneNo());
        }*/

        Map<String, Integer> indirectResultMap = directRelationDataBean.getMap2();
        //判断该标签是否包含一度数据
        Set<Map.Entry<String, Integer>> indirectResultSet = indirectResultMap.entrySet();
        for (Map.Entry<String, Integer> en : indirectResultSet) {
            if (directMarks.contains(en.getKey())) {
                for (IndexData indexData : indexDatas) {
                    if (indexData.getIndexName().equals(IndexNameEnum.fromValue(en.getKey()))) {
                        indexData.setIndirect(en.getValue());
                    }
                }
            } else {
                IndexData indexData = new IndexData();
                indexData.setMemberId(directRelationDataBean.getMemberId());
                indexData.setOrderNo(orderNo);
                indexData.setMobile(directRelationDataBean.getPhoneNo());
                indexData.setIndirect(en.getValue());
                indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
                indexData.setCreateTime(createDatetime);
                indexDatas.add(indexData);
            }
        }
        LOGGER.info("dealBasicDataByOrderNo insertPhonetagIndex orderNo is {}", orderNo);
        insertPhonetagIndex(indexDatas);
        if (indexDatas != null) {
            indexDatas.clear();
            indexDatas = null;
        }

        if (directMarks != null) {
            directMarks.clear();
            directMarks = null;
        }

        if (indirectResultMap != null) {
            indirectResultMap.clear();
            indirectResultMap = null;
        }

        if (directResultMap != null) {
            directResultMap.clear();
            directResultMap = null;
        }
        if (directSet != null) {
            directSet.clear();
            directSet = null;
        }
        if (indirectResultSet != null) {
            indirectResultSet.clear();
            indirectResultSet = null;
        }

    }

    protected static String getRid(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof OResultSet) {
            OResultSet ors = (OResultSet) obj;
            if (ors != null && !ors.isEmpty()) {
                return ((ODocument) ors.get(0)).getIdentity().toString();
            }
        } else if (obj instanceof ODocument) {
            return ((ODocument) obj).getIdentity().toString();
        }
        return null;
    }

    public static void getApplyphonetag(List<ApplyAndOrderDate> applyNoList, boolean isAllData) {
        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<ApplyAndOrderDate> applyNos = new ArrayList<ApplyAndOrderDate>();
        try {
            if (!isAllData) {
                for (ApplyAndOrderDate applyAndOrderDate : applyNoList) {
                    pstmt = conn.prepareStatement("select orderinfo.orderNo as orderNo from (MATCH{class:Apply,where: (applyNo = ?)}-ApplyHasOrder->{as:orderinfo} return orderinfo)");
                    pstmt.setString(1, applyAndOrderDate.getNo());
                    rs = pstmt.executeQuery();
                    if (rs.next()) {
                        //如果此申请单号有订单号则暂不跑，等订单传递来再进行跑批
                    } else {
                        applyNos.add(applyAndOrderDate);
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                LOGGER.error("getApplyphonetag applyNoList rs.close have e {}", e);
            }

            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                LOGGER.error("getApplyphonetag applyNoList pstmt.close have e {}", e);
            }
        }
        if (!isAllData) {
            LOGGER.info("getApplyphonetag size" + applyNos.size());
        } else {
            LOGGER.info("getApplyphonetag size" + applyNoList.size());
        }


        if (isAllData) {
            if (null != applyNoList && applyNoList.size() > 0) {
                ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                dealAllBasicDataByApplyList(applyNoList, tx);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }
            }
        } else {
            if (null != applyNos && applyNos.size() > 0) {
                SqlUtils.getApplyphonetag(applyNos, conn);

                if (applyNos != null) {
                    applyNos.clear();
                    applyNos = null;
                }
            }
        }
        if (applyNoList != null) {
            applyNoList.clear();
            applyNoList = null;
        }
        try {
            if (null != conn) {
                conn.close();
            }
        } catch (Exception e) {
            LOGGER.error("getApplyphonetag applyNoList conn.close  have e {}", e);
        }

    }

    public static void getOrderphonetag(List<ApplyAndOrderDate> orderNoList, boolean isAllData) {
        Connection conn = (OrientJdbcConnection) OrientDbUtils.getConnection(ConfigUtils.getProperty("orientDbSourceUrl"),
                ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<ApplyAndOrderDate> applyNosHasOrders = new ArrayList<ApplyAndOrderDate>();
        List<ApplyAndOrderDate> orderNos = new ArrayList<ApplyAndOrderDate>();
        try {
            for (ApplyAndOrderDate applyAndOrderDate : orderNoList) {
                pstmt = conn.prepareStatement("select applyinfo.applyNo as applyNo from (MATCH{class:Order,where: (orderNo = ?)}<-ApplyHasOrder-{as:applyinfo} return applyinfo)");
                pstmt.setString(1, applyAndOrderDate.getNo());
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    if (null != rs.getString("applyNo")) {
                        applyAndOrderDate.setNo(rs.getString("applyNo"));
                        applyNosHasOrders.add(applyAndOrderDate);
                    }
                } else {
                    orderNos.add(applyAndOrderDate);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                LOGGER.error("getOrderphonetag applyNoList rs.close have e {}", e);
            }

            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                LOGGER.error("getOrderphonetag applyNoList pstmt.close have e {}", e);
            }
        }

        LOGGER.info("getOrderphonetag size" + orderNos.size());
        if (null != orderNos && orderNos.size() > 0) {
            if (isAllData) {
                ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                dealAllBasicDataByOrderList(orderNos, tx);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }
            } else {
                SqlUtils.getOrderphonetag(orderNos, conn);
            }
            if (orderNos != null) {
                orderNos.clear();
                orderNos = null;
            }
            if (orderNoList != null) {
                orderNoList.clear();
                orderNoList = null;
            }
        }

        LOGGER.info("getOrderphonetag size" + applyNosHasOrders.size());
        if (null != applyNosHasOrders && applyNosHasOrders.size() > 0) {
            if (!isAllData) {
                SqlUtils.getApplyNosHasOrdersphonetag(applyNosHasOrders, conn);
            }
            if (applyNosHasOrders != null) {
                applyNosHasOrders.clear();
                applyNosHasOrders = null;
            }
        }

        try {
            if (null != conn) {
                conn.close();
            }
        } catch (Exception e) {
            LOGGER.error("getOrderphonetag applyNoList conn.close have e {}", e);
        }
    }

    public static void getApplyphonetag(List<ApplyAndOrderDate> applyNos, Connection conn) {
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        int applyNosSize = applyNos.size();
        for (int i = 0; i < applyNosSize; i++) {
            List<IndexData> indexDatas = new ArrayList<IndexData>();
            String applyNo = applyNos.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                sql = "select count(memberHasPhoneCalltoPhone) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "       where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        "       applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                map = setPhoneTagIndexDatas(rs, indexDatas, list, map, applyNos.get(i).getCreateDatetime());

                LOGGER.info("getApplyphonetag 查询一度 end i is " + i);

                //查找出一度联系人的电话号码
                sql = " select applyInfo.applyNo as applyNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
                insertPhonetagIndex(indexDatas);
                if (indexDatas != null) {
                    indexDatas.clear();
                    indexDatas = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyphonetag direct rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyphonetag direct pstmt.close() have e {}", e);
                }
            }

            if (list != null) {
                list.clear();
                list = null;
            }

            if (map != null) {
                map.clear();
                map = null;
            }
        }

        int applyNosNum = applyNos.size();
        for (int i = 0; i < applyNosNum; i++) {
            String applyNo = applyNos.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from " +
                        "      (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}<-PhoneHasApply-{as:phone}<-HasPhone-{as: member}-MemberHasDevice->{as: device}" +
                        "       <-MemberHasDevice-{as:deviceMember} return applyInfo,phone,device,member,deviceMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);

                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num",applyNos.get(i).getCreateDatetime());

                LOGGER.info("getApplyphonetag 查询同设备客户 end i is " + i);

                //同IP客户个数
                sql = "select count(ipMember) as direct,applyInfo.applyNo as applyNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from " +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}<-PhoneHasApply-{as:phone}<-HasPhone-{as: member}-MemberHasIp->{as: ip}" +
                        "    <-MemberHasIp-{as:ipMember} return applyInfo,phone,ip,member,ipMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num",applyNos.get(i).getCreateDatetime());

                //连接设备的个数
                sql = "select count(device) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "     <-PhoneHasApply-{as:phone}<-HasPhone-{as:member}-MemberHasDevice->{as:device}   return applyInfo,phone,member,device)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num",applyNos.get(i).getCreateDatetime());

                //连接不同ip的个数
                sql = "select count(ip) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)} " +
                        "   <-PhoneHasApply-{as:phone}<-HasPhone-{as:member}-MemberHasIp->{as:ip}   return applyInfo,phone,member,ip)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);

                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num",applyNos.get(i).getCreateDatetime());

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)} " +
                        "      <-PhoneHasApply-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{}-ApplyHasStore->{as:storeinfo}   return applyInfo,phone,member,storeinfo)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num",applyNos.get(i).getCreateDatetime());

                //连接不同申请件数
                sql = "select count(applys) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "        <-PhoneHasApply-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{as:applys}   return applyInfo,phone,member,applys)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num",applyNos.get(i).getCreateDatetime());

                //连接不同订单数
                sql = "select count(orders) as direct,applyInfo.applyNo as applyNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "                    <-PhoneHasApply-{as:phone}<-HasPhone-{as:member}-MemberHasOrder->{as:orders}   return applyInfo,phone,member,orders)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num",applyNos.get(i).getCreateDatetime());

                //联系过件客户个数

                sql = "select applyInfo.applyNo as applyNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Apply, as:applyInfo," +
                        "     where:(applyNo=?)}<-PhoneHasApply-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{as: members} RETURN " +
                        "    applyInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn,applyNos.get(i).getCreateDatetime());

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyphonetag rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyphonetag pstmt.close() have e {}", e);
                }
            }
        }

        LOGGER.info("getApplyphonetag insertDeviceAndIpIndex start");
        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas);
        insertMemberIndex(memberIndexDatas);

        if (deviceIndexDatas != null) {
            deviceIndexDatas.clear();
            deviceIndexDatas = null;
        }

        if (memberIndexDatas != null) {
            memberIndexDatas.clear();
            memberIndexDatas = null;
        }

        if (ipIndexDatas != null) {
            ipIndexDatas.clear();
            ipIndexDatas = null;
        }
    }

    public static void getOrderphonetag(List<ApplyAndOrderDate> orderNos, Connection conn) {
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        int orderNosNum = orderNos.size();
        for (int i = 0; i < orderNosNum; i++) {
            List<IndexData> indexDatas = new ArrayList<IndexData>();
            String orderNo = orderNos.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                sql = "select count(memberHasPhoneCalltoPhone) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Order, as:orderInfo," +
                        "           where:(orderNo=?)}<-PhoneHasOrder-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        "            orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                map = setPhoneTagIndexDatas(rs, indexDatas, list, map, orderNos.get(i).getCreateDatetime());

                LOGGER.info("getOrderphonetag 查询一度 end i is " + i);

                //查找出一度联系人的电话号码
                sql = " select orderInfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Order, as:orderInfo, " +
                        "where:(orderNo=?)}<-PhoneHasOrder-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
                insertPhonetagIndex(indexDatas);
                if (indexDatas != null) {
                    indexDatas.clear();
                    indexDatas = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getOrderphonetag direct rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getOrderphonetag direct pstmt.close() have e {}", e);
                }
            }

            if (list != null) {
                list.clear();
                list = null;
            }

            if (map != null) {
                map.clear();
                map = null;
            }
        }

        int orderNoNum = orderNos.size();
        for (int i = 0; i < orderNoNum; i++) {
            String orderNo = orderNos.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from " +
                        "  (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}<-PhoneHasOrder-{as:phone}<-HasPhone-{as: member}-MemberHasDevice->{as: device}" +
                        "   <-MemberHasDevice-{as:deviceMember} return orderInfo,phone,device,member,deviceMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num", orderNos.get(i).getCreateDatetime());

                LOGGER.info("getOrderphonetag 查询同设备客户 end i is " + i);

                //同IP客户个数
                sql = "select count(ipMember) as direct,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from " +
                        "    (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}<-PhoneHasOrder-{as:phone}<-HasPhone-{as: member}-MemberHasIp->{as: ip}" +
                        "    <-MemberHasIp-{as:ipMember} return orderInfo,phone,ip,member,ipMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num", orderNos.get(i).getCreateDatetime());

                //连接设备的个数
                sql = "select count(device) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasDevice->{as:device}   return orderInfo,phone,member,device)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num", orderNos.get(i).getCreateDatetime());

                //连接不同ip的个数
                sql = "select count(ip) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasIp->{as:ip}   return orderInfo,phone,member,ip)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num", orderNos.get(i).getCreateDatetime());

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "       <-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{}-ApplyHasStore->{as:storeinfo}   return orderInfo,phone,member,storeinfo)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num", orderNos.get(i).getCreateDatetime());

                //连接不同申请件数
                sql = "select count(applys) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "   <-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{as:applys}   return orderInfo,phone,member,applys)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num", orderNos.get(i).getCreateDatetime());

                //连接不同订单数
                sql = "select count(orders) as direct,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:Order, as:orderInfo,where: (orderNo = ?)}" +
                        "  <-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasOrder->{as:orders}   return orderInfo,phone,member,orders)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num", orderNos.get(i).getCreateDatetime());

                //联系过件客户个数
                sql = "select orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Order, as:orderInfo,where: (orderNo = ?)} " +
                        "   <-PhoneHasOrder-{as:phone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{as: members} RETURN " +
                        "   orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(orderNo, pstmt);
                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn, orderNos.get(i).getCreateDatetime());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getOrderphonetag rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getOrderphonetag pstmt.close() have e {}", e);
                }
            }
        }
        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas);
        insertMemberIndex(memberIndexDatas);
        if (deviceIndexDatas != null) {
            deviceIndexDatas.clear();
            deviceIndexDatas = null;
        }

        if (memberIndexDatas != null) {
            memberIndexDatas.clear();
            memberIndexDatas = null;
        }

        if (ipIndexDatas != null) {
            ipIndexDatas.clear();
            ipIndexDatas = null;
        }
    }

    public static void getApplyNosHasOrdersphonetag(List<ApplyAndOrderDate> applyNosHasOrders, Connection conn) {
        List<IndexData> deviceIndexDatas = new ArrayList<IndexData>();
        List<IndexData> ipIndexDatas = new ArrayList<IndexData>();
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        String sql = "";
        int applyNosHasOrdersSize = applyNosHasOrders.size();
        for (int i = 0; i < applyNosHasOrdersSize; i++) {
            List<IndexData> indexDatas = new ArrayList<IndexData>();
            String applyNo = applyNosHasOrders.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            //一度联系人电话标签
            List<String> list = new ArrayList<String>();
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                //一度
                sql = "select count(memberHasPhoneCalltoPhone) as direct,applyInfo.applyNo as applyNo,orderinfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        " where:(applyNo=?)}-ApplyHasOrder->{as:orderinfo}<-PhoneHasOrder-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} RETURN " +
                        " applyInfo,orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark) group by memberHasPhoneCalltoPhoneMark.mark order by count desc";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                setPhoneTagIndexDatas(rs, indexDatas, list, map,applyNosHasOrders.get(i).getCreateDatetime());

                LOGGER.info("getApplyNosHasOrdersphonetag 查询一度 end i is " + i);

                //查找出一度联系人的电话号码
                sql = " select applyInfo.applyNo as applyNo,orderinfo.orderNo as orderNo, member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Apply, as:applyInfo, " +
                        "where:(applyNo=?)}-ApplyHasOrder->{as:orderinfo}<-PhoneHasOrder-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  applyInfo,orderinfo,member,memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                indexDatas = queryIndirect(conn, rs, indexDatas, list, (IndexData) map.get("firstIndexData"));
                insertPhonetagIndex(indexDatas);
                if (indexDatas != null) {
                    indexDatas.clear();
                    indexDatas = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyNosHasOrdersphonetag direct rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyNosHasOrdersphonetag direct pstmt.close() have e {}", e);
                }
            }
            if (list != null) {
                list.clear();
                list = null;
            }
            if (map != null) {
                map.clear();
                map = null;
            }
        }

        LOGGER.info("getApplyNosHasOrdersphonetag insert start");

        int applyNosHasOrdersNum = applyNosHasOrders.size();
        for (int i = 0; i < applyNosHasOrdersNum; i++) {
            String applyNo = applyNosHasOrders.get(i).getNo();
            ResultSet rs = null;
            PreparedStatement pstmt = null;
            try {
                //同设备客户个数
                sql = "select count(deviceMember) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,device.deviceId as deviceId from" +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as: member}-MemberHasDevice->{as: device}" +
                        "   <-MemberHasDevice-{as:deviceMember} return applyInfo,orderInfo,phone,device,member,deviceMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                deviceIndexDatas = setMemberIndexDatas(rs, deviceIndexDatas, "equal_device_member_num",applyNosHasOrders.get(i).getCreateDatetime());

                LOGGER.info("getApplyNosHasOrdersphonetag 查询同设备客户 end i is " + i);

                //同IP客户个数
                sql = "select count(ipMember) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,phone.phone as phone,ip.ip as ip from" +
                        "   (MATCH{class:Apply, as:applyInfo,where: (applyNo = ?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as: member}-MemberHasIp->{as: ip}" +
                        "   <-MemberHasIp-{as:ipMember} return applyInfo,orderInfo,phone,ip,member,ipMember)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                ipIndexDatas = setMemberIndexDatas(rs, ipIndexDatas, "equal_ip_member_num",applyNosHasOrders.get(i).getCreateDatetime());

                //连接设备的个数
                sql = "select count(device) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasDevice->{as:device}   return applyInfo,orderInfo,phone,member,device)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_device_num",applyNosHasOrders.get(i).getCreateDatetime());

                //连接不同ip的个数
                sql = "select count(ip) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasIp->{as:ip}   return applyInfo,orderInfo,phone,member,ip)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_ip_num",applyNosHasOrders.get(i).getCreateDatetime());

                //连接不同商户个数
                sql = "select count(storeinfo) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{}-ApplyHasStore->{as:storeinfo}   return applyInfo,orderInfo,phone,member,storeinfo)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_merchant_num",applyNosHasOrders.get(i).getCreateDatetime());

                //连接不同申请件数
                sql = "select count(applys) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasApply->{as:applys}   return applyInfo,orderInfo,phone,member,applys)";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_appl_num",applyNosHasOrders.get(i).getCreateDatetime());

                //连接不同订单数
                sql = "select count(orders) as direct,applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,phone.phone as phone,member.memberId as memberId from (MATCH{class:apply, as:applyInfo,where: (applyNo = ?)}" +
                        "-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:phone}<-HasPhone-{as:member}-MemberHasOrder->{as:orders}   return applyInfo,orderInfo,phone,member,orders) ";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setMemberIndexDatas(rs, memberIndexDatas, "has_order_num",applyNosHasOrders.get(i).getCreateDatetime());

                //联系过件和拒件客户个数
                sql = "select applyInfo.applyNo as applyNo,orderInfo.orderNo as orderNo,member.memberId as memberId,memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as phone1,members.memberId as memberIds from (MATCH {class:Apply, as:applyInfo,\n" +
                        "where:(applyNo=?)}-ApplyHasOrder->{as:orderInfo}<-PhoneHasOrder-{as:applyPhone}<-HasPhone-{as: member}-HasPhone->{as:memberHasPhone}-CallTo-{as:memberHasPhoneCalltoPhone}<-HasPhone-{as: members} RETURN \n" +
                        "applyInfo,orderInfo,member,memberHasPhone,memberHasPhoneCalltoPhone,members) ";
                pstmt = conn.prepareStatement(sql);
                rs = getResultSet(applyNo, pstmt);
                memberIndexDatas = setOrderMemberIndexDatas(rs, memberIndexDatas, conn,applyNosHasOrders.get(i).getCreateDatetime());

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyNosHasOrdersphonetag  rs.close() have e {}", e);
                }

                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("getApplyNosHasOrdersphonetag  pstmt.close() have e {}", e);
                }
            }
        }

        LOGGER.info("getApplyNosHasOrdersphonetag insertDeviceAndIpIndex start");
        insertDeviceAndIpIndex(deviceIndexDatas, ipIndexDatas);
        insertMemberIndex(memberIndexDatas);
        if (deviceIndexDatas != null) {
            deviceIndexDatas.clear();
            deviceIndexDatas = null;
        }

        if (memberIndexDatas != null) {
            memberIndexDatas.clear();
            memberIndexDatas = null;
        }

        if (ipIndexDatas != null) {
            ipIndexDatas.clear();
            ipIndexDatas = null;
        }
    }

    private static ResultSet getResultSet(String applyNo, PreparedStatement pstmt) throws Exception {
        pstmt.setString(1, applyNo);
        ResultSet rs = pstmt.executeQuery();
        return rs;
    }


    private static List<IndexData> queryIndirect(Connection conn, ResultSet
            rs, List<IndexData> indexDatas, List<String> list, IndexData firstIndexData) throws Exception {
        Set<String> directSet = new HashSet<String>();
        HashMap<String, String> indirectMap = new HashMap<String, String>();
        String memberHasPhoneCalltoPhone = "";
        String sql = "";
        ResultSet rs1 = null;
        PreparedStatement pstmt = null;
        while (rs.next()) {
            memberHasPhoneCalltoPhone = rs.getString("memberHasPhoneCalltoPhone");
            directSet.add(memberHasPhoneCalltoPhone);

            if (checkPhone(memberHasPhoneCalltoPhone)){
                //找一度联系人的一度联系人
                sql = " select memberHasPhone.phone as phone,memberHasPhoneCalltoPhone.phone as memberHasPhoneCalltoPhone ,memberHasPhoneCalltoPhoneMark.mark as mark from (MATCH {class:Phone, as:memberHasPhone, " +
                        "where:(phone=?)}-CallTo-{as:memberHasPhoneCalltoPhone}-HasPhoneMark->{as:memberHasPhoneCalltoPhoneMark} " +
                        "RETURN  memberHasPhone,memberHasPhoneCalltoPhone, memberHasPhoneCalltoPhoneMark)";
                pstmt = conn.prepareStatement(sql);
                rs1 = getResultSet(memberHasPhoneCalltoPhone, pstmt);
                while (rs1.next()) {
                    indirectMap.put(rs1.getString("memberHasPhoneCalltoPhone"), rs1.getString("mark"));
                }
            }
        }

        try {
            if (rs1 != null) {
                rs1.close();
            }
        } catch (Exception e) {
            LOGGER.error("queryIndirect rs1 close have e {}", e);
        }

        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (Exception e) {
            LOGGER.error("queryIndirect pstmt close have e {}", e);
        }

        if (null != indirectMap && indirectMap.size() > 0) {
            //判断是否包含一度联系人
            for (String str : directSet) {
                if (indirectMap.containsKey(str)) {
                    indirectMap.remove(str);
                }
            }
            //判断是否包含自身
            if (indirectMap.containsKey(firstIndexData.getMobile())) {
                indirectMap.remove(firstIndexData.getMobile());
            }

            //统计各种标签的二度联系人
            Set<Map.Entry<String, String>> set = indirectMap.entrySet();
            Map<String, Integer> indirectResultMap = new HashMap<String, Integer>();
            for (Map.Entry<String, String> en : set) {
                if (indirectResultMap.containsKey(en.getValue())) {
                    indirectResultMap.put(en.getValue(), indirectResultMap.get(en.getValue()) + 1);
                } else {
                    indirectResultMap.put(en.getValue(), 1);
                }
            }

            //判断该标签是否包含一度数据
            Set<Map.Entry<String, Integer>> indirectResultSet = indirectResultMap.entrySet();
            for (Map.Entry<String, Integer> en : indirectResultSet) {
                if (list.contains(en.getKey())) {
                    for (IndexData indexData : indexDatas) {
                        if (indexData.getIndexName().equals(IndexNameEnum.fromValue(en.getKey()))) {
                            indexData.setIndirect(en.getValue());
                        }
                    }
                } else {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(firstIndexData.getMemberId());
                    indexData.setApplyNo(firstIndexData.getApplyNo());
                    indexData.setOrderNo(firstIndexData.getOrderNo());
                    indexData.setMobile(firstIndexData.getMobile());
                    indexData.setIndirect(en.getValue());
                    indexData.setIndexName(IndexNameEnum.fromValue(en.getKey()));
                    indexData.setCreateTime(firstIndexData.getCreateTime());
                    indexDatas.add(indexData);
                }
            }
        }

        return indexDatas;
    }

    private static List<IndexData> setMemberIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, String indexName, String createTime) {
        try {
            while (rs.next()) {
                if (0 != rs.getInt("direct")) {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(rs.getInt("memberId"));
                    indexData.setMobile(rs.getString("phone"));
                    if (null != rs.getString("applyNo")) {
                        indexData.setApplyNo(rs.getString("applyNo"));
                    }
                    if (null != rs.getString("orderNo")) {
                        indexData.setOrderNo(rs.getString("orderNo"));
                    }
                    if (null != rs.getString("ip")) {
                        indexData.setIp(rs.getString("ip"));
                    }
                    if (null != rs.getString("deviceId")) {
                        indexData.setDeviceId(rs.getString("deviceId"));
                    }
                    indexData.setDirect(rs.getInt("direct"));
                    indexData.setCreateTime(createTime);
                    indexData.setIndexName(indexName);

                    memberIndexDatas.add(indexData);
                    indexData = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberIndexDatas;
    }

    private static Map<String, Object> setPhoneTagIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, List<String> list, Map<String, Object> map, String createTime) {
        IndexData firstIndexData = new IndexData();
        try {
            while (rs.next()) {
                if (null != rs.getString("mark")) {
                    IndexData indexData = new IndexData();
                    indexData.setMemberId(rs.getInt("memberId"));
                    indexData.setMobile(rs.getString("phone"));
                    if (null != rs.getString("applyNo")) {
                        indexData.setApplyNo(rs.getString("applyNo"));
                    }
                    if (null != rs.getString("orderNo")) {
                        indexData.setOrderNo(rs.getString("orderNo"));
                    }
                    indexData.setDirect(rs.getInt("direct"));
                    indexData.setIndexName(IndexNameEnum.fromValue(rs.getString("mark").toString()));
                    indexData.setCreateTime(createTime);
                    memberIndexDatas.add(indexData);
                    list.add(rs.getString("mark"));
                    if (null != firstIndexData) {
                        firstIndexData = indexData;
                    }

                    indexData = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        map.put("firstIndexData", firstIndexData);
        return map;
    }

    private static List<IndexData> setOrderMemberIndexDatas(ResultSet rs, List<IndexData> memberIndexDatas, Connection conn, String createTime) {
        ResultSet rs1 = null;
        PreparedStatement pstmt = null;

        //一度联系人过件个数
        int contactAccept = 0;
        //一度联系人拒件个数
        int contactRefuse = 0;
        //一度联系人逾期个数
        int contactOverdue = 0;
        //一度联系人黑名单个数
        int contactBlack = 0;

        boolean flag = true;
        try {
            //此时一度联系人可能为多个，统计所有一度联系人符合条件的订单总和数
            IndexData indexData = new IndexData();
            indexData.setCreateTime(createTime);
            while (rs.next()) {
                if (flag) {
                    indexData.setMemberId(rs.getInt("memberId"));
                    indexData.setMobile(rs.getString("phone"));
                    if (null != rs.getString("applyNo")) {
                        indexData.setApplyNo(rs.getString("applyNo"));
                    }
                    if (null != rs.getString("orderNo")) {
                        indexData.setOrderNo(rs.getString("orderNo"));
                    }
                    flag = false;//只需要赋值一次，提高效率
                }
                int memberId = rs.getInt("memberIds");
                pstmt = conn.prepareStatement("select out('MemberHasOrder').amount[0] as amount,out('MemberHasOrder').orderNo[0] as orderNo,out('MemberHasOrder').status[0] as status," +
                        "out('MemberHasOrder').originalStatus[0] as originalStatus from member where memberId = ? order by createDateTime desc limit 1");
                pstmt.setInt(1, memberId);
                rs1 = pstmt.executeQuery();
                if (rs1.next()) {
                    if ("1051".equals(rs1.getString("originalStatus"))) {
                        contactAccept++;
                    }
                    if ("1013".equals(rs1.getString("originalStatus"))) {
                        contactRefuse++;
                    }
                }

                pstmt = conn.prepareStatement("select isBlack as isBlack,isOverdue as isOverdue from member where memberId =?");
                pstmt.setInt(1, memberId);
                rs1 = pstmt.executeQuery();
                if (rs1.next()) {
                    if (true == rs1.getBoolean("isOverdue")) {
                        contactOverdue++;
                    }
                    if (true == rs1.getBoolean("isBlack")) {
                        contactBlack++;
                    }
                }
            }

            if (0 != contactAccept) {
                indexData.setDirect(contactAccept);
                indexData.setIndexName("contact_accept_member_num");
                memberIndexDatas.add(indexData);
            }
            if (0 != contactRefuse) {
                indexData.setDirect(contactRefuse);
                indexData.setIndexName("contact_refuse_member_num");
                memberIndexDatas.add(indexData);
            }

            if (0 != contactOverdue) {
                indexData.setDirect(contactOverdue);
                indexData.setIndexName("contact_overdue_member_num");
                memberIndexDatas.add(indexData);
            }
            if (0 != contactBlack) {
                indexData.setDirect(contactBlack);
                indexData.setIndexName("contact_black_member_num");
                memberIndexDatas.add(indexData);
            }
            indexData = null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs1 != null) {
                    rs1.close();
                }
            } catch (Exception e) {
                LOGGER.error("setOrderMemberIndexDatas direct rs.close() have e {}", e);
            }

            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                LOGGER.error("setOrderMemberIndexDatas direct pstmt.close() have e {}", e);
            }
        }

        return memberIndexDatas;
    }

    private static void insertPhonetagIndex(List<IndexData> indexDatas) {
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != indexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into phonetag_index (member_id, apply_no, order_no,mobile,index_name,direct,indirect,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,?,now())");

                for (int i = 0; i < indexDatas.size(); i++) {
                    pstmt.setLong(1, indexDatas.get(i).getMemberId());
                    pstmt.setString(2, indexDatas.get(i).getApplyNo());
                    pstmt.setString(3, indexDatas.get(i).getOrderNo());
                    pstmt.setString(4, indexDatas.get(i).getMobile());
                    pstmt.setString(5, indexDatas.get(i).getIndexName());
                    pstmt.setLong(6, indexDatas.get(i).getDirect());
                    pstmt.setLong(7, indexDatas.get(i).getIndirect());
                    pstmt.setString(8, indexDatas.get(i).getCreateTime());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertPhonetagIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertPhonetagIndex mysqlConn.close have e {}", e);
                }
            }
        }

    }

    private static void insertDeviceAndIpIndex
            (List<IndexData> deviceIndexDatas, List<IndexData> ipIndexDatas) {
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != deviceIndexDatas && null != ipIndexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into device_index (member_id, apply_no, order_no,mobile,deviceId,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,?,now())");

                for (int i = 0; i < deviceIndexDatas.size(); i++) {
                    pstmt.setLong(1, deviceIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, deviceIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, deviceIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, deviceIndexDatas.get(i).getMobile());
                    pstmt.setString(5, deviceIndexDatas.get(i).getDeviceId());
                    pstmt.setString(6, deviceIndexDatas.get(i).getIndexName());
                    pstmt.setLong(7, deviceIndexDatas.get(i).getDirect());
                    pstmt.setString(8, deviceIndexDatas.get(i).getCreateTime());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();

                pstmt = mysqlConn.prepareStatement("insert into ip_index (member_id, apply_no, order_no,mobile,ip,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,?,now())");

                for (int i = 0; i < ipIndexDatas.size(); i++) {
                    pstmt.setLong(1, ipIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, ipIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, ipIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, ipIndexDatas.get(i).getMobile());
                    pstmt.setString(5, ipIndexDatas.get(i).getIp());
                    pstmt.setString(6, ipIndexDatas.get(i).getIndexName());
                    pstmt.setLong(7, ipIndexDatas.get(i).getDirect());
                    pstmt.setString(8, deviceIndexDatas.get(i).getCreateTime());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertDeviceAndIpIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertDeviceAndIpIndex mysqlConn.close have e {}", e);
                }
            }
        }

    }

    private static void insertMemberIndex(List<IndexData> memberIndexDatas) {
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != memberIndexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into member_index (member_id, apply_no, order_no,mobile,index_name,direct,create_time,update_time) " +
                        "values(?,?,?,?,?,?,?,now())");

                for (int i = 0; i < memberIndexDatas.size(); i++) {
                    pstmt.setLong(1, memberIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, memberIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, memberIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, memberIndexDatas.get(i).getMobile());
                    pstmt.setString(5, memberIndexDatas.get(i).getIndexName());
                    pstmt.setLong(6, memberIndexDatas.get(i).getDirect());
                    pstmt.setString(7, memberIndexDatas.get(i).getCreateTime());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertMemberIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("insertMemberIndex mysqlConn.close have e {}", e);
                }
            }
        }
    }

    public static void queryBasicData(String date) {
        boolean isAllDataQueryFlag = true;
        if (!StringUtils.isBlank(date)) {
            isAllDataQueryFlag = false;
        }

        if (!delBasicData(date)) {
            LOGGER.error("delBasicData is fail");
            return;
        }

        Connection mysqlBusinesConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbBusinessSourceUrl"),
                ConfigUtils.getProperty("mysqlDbBusinessUserName"), ConfigUtils.getProperty("mysqlDbBusinessUserPassword"));

        ExecutorService es = new ThreadPoolExecutor(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"))
                , Integer.parseInt(ConfigUtils.getProperty("allDataImportMainMaximumPoolSize")),
                Long.parseLong(ConfigUtils.getProperty("allDataImportMainKeepAliveTime")), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainQueueLength"))));

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int applyCount = 0;
        int orderCount = 0;
        try {
            //查询总数
            if (!isAllDataQueryFlag) {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM apply_info where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
            } else {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM apply_info");
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                applyCount = rs.getInt("total");
            }

            //查询总数
            if (!isAllDataQueryFlag) {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM money_box_order where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
            } else {
                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM money_box_order");
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                orderCount = rs.getInt("total");
            }

            int startIndex = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainStartIndex"));
            int allNum = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"));
            int applimitNum = (applyCount % allNum == 0) ? applyCount / allNum : (applyCount / allNum + 1);
            int orderlimitNum = (orderCount % allNum == 0) ? orderCount / allNum : (orderCount / allNum + 1);
            for (int i = startIndex; i < allNum; i++) {
                List<ApplyAndOrderDate> applyNos = new ArrayList<ApplyAndOrderDate>();
                List<ApplyAndOrderDate> orderNos = new ArrayList<ApplyAndOrderDate>();
                if (!isAllDataQueryFlag) {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no,created_datetime FROM apply_info where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? order by id limit ?,?");
                    pstmt.setString(1, date);
                    pstmt.setInt(2, i * applimitNum);
                    pstmt.setInt(3, applimitNum);
                } else {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no,created_datetime FROM apply_info order by id limit ?,?");
                    pstmt.setInt(1, i * applimitNum);
                    pstmt.setInt(2, applimitNum);
                }
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    ApplyAndOrderDate applyAndOrderDate = new ApplyAndOrderDate();
                    applyAndOrderDate.setCreateDatetime(rs.getString("created_datetime"));
                    applyAndOrderDate.setNo(rs.getString("apply_no"));
                    applyNos.add(applyAndOrderDate);
                }
                if (!isAllDataQueryFlag) {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT order_no,created_datetime FROM money_box_order where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? order by id limit ?,?");
                    pstmt.setString(1, date);
                    pstmt.setInt(2, i * orderlimitNum);
                    pstmt.setInt(3, orderlimitNum);
                } else {
                    pstmt = mysqlBusinesConn.prepareStatement("SELECT order_no,created_datetime FROM money_box_order order by id limit ?,?");
                    pstmt.setInt(1, i * orderlimitNum);
                    pstmt.setInt(2, orderlimitNum);
                }
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    ApplyAndOrderDate applyAndOrderDate = new ApplyAndOrderDate();
                    applyAndOrderDate.setCreateDatetime(rs.getString("created_datetime"));
                    applyAndOrderDate.setNo(rs.getString("order_no"));
                    orderNos.add(applyAndOrderDate);
                }

                BasicDataBatchTask basicDataBatchTask = new BasicDataBatchTask();
                basicDataBatchTask.setApplyNos(applyNos);
                basicDataBatchTask.setOrderNos(orderNos);
                basicDataBatchTask.setAllData(isAllDataQueryFlag);
                es.submit(basicDataBatchTask);
                applyNos.clear();
                applyNos = null;
                orderNos.clear();
                orderNos = null;
            }
            LOGGER.info("已经开启所有的子线程");
            es.shutdown();
            LOGGER.info("shutdown()：启动一次顺序关闭，执行以前提交的任务，但不接受新任务。");
            while (true) {
                if (es.isTerminated()) {
                    LOGGER.info("所有的子线程都结束了！");
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                if (null != pstmt) {
                    pstmt.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                if (mysqlBusinesConn != null) {
                    mysqlBusinesConn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean delBasicData(String date) {
        boolean isAllDataQueryFlag = false;

        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));
        if (StringUtils.isBlank(date)) {
            isAllDataQueryFlag = true;
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");//小写的mm表示的是分钟
            try {
                Date dateFormat = sdf.parse(date);
                Calendar calendar = new GregorianCalendar();
                calendar.setTime(dateFormat);
                calendar.add(calendar.DATE, 1);//把日期往后增加一天.整数往后推,负数往前移动
                dateFormat = calendar.getTime();
                date = sdf.format(dateFormat);
            } catch (Exception e) {
                LOGGER.error("date convert fail e is {}" + e);
                return false;
            }
        }

        PreparedStatement pstmt = null;
        try {
            if (!isAllDataQueryFlag) {
                pstmt = mysqlConn.prepareStatement("delete FROM phonetag_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM ip_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM device_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM member_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ?");
                pstmt.setString(1, date);
                pstmt.executeUpdate();
            } else {
                pstmt = mysqlConn.prepareStatement("delete FROM phonetag_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM ip_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM device_index");
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM member_index");
                pstmt.executeUpdate();
            }
        } catch (Exception e) {
            LOGGER.info("delBasicData mysqlConn.close have e {}", e);
            return false;
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception e) {
                LOGGER.info("delBasicData pstmt.close have e {}", e);
            }
            try {
                if (mysqlConn != null) {
                    mysqlConn.close();
                }
            } catch (Exception e) {
                LOGGER.info("delBasicData mysqlConn.close have e {}", e);
            }

            return true;
        }
    }
}
