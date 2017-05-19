package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.ConstantHelper;
import cn.memedai.orientdb.fraud.statistics.bean.*;
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
     * 校验phone合法性
     *
     * @param phone
     * @return
     */
    private static Boolean checkPhone(String phone) {
        if (StringUtils.isBlank(phone)) {
            LOGGER.info("checkPhone invalid phone is {}", phone);
            return false;
        }

        if (phone.length() < ConstantHelper.BUSINESS_PHONE_LENGTH) {
            LOGGER.info("checkPhone invalid phone is {}", phone);
            return false;
        }

        if (phone.length() >= 2) {
            if (ConstantHelper.BUSINESS_PHONE_1.equals(phone.substring(0, 1))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_2.equals(phone.substring(0, 1))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
        }

        if (phone.length() >= 3) {
            if (ConstantHelper.BUSINESS_PHONE_3.equals(phone.substring(0, 2))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_4.equals(phone.substring(0, 2))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
        }

        if (phone.length() >= 5) {
            if (ConstantHelper.BUSINESS_PHONE_5.equals(phone.substring(0, 4))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_6.equals(phone.substring(0, 4))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_7.equals(phone.substring(0, 4))) {
                LOGGER.info("checkPhone invalid phone is {}", phone);
                return false;
            }
        }

        if (ConstantHelper.BUSINESS_PHONE_7.equals(phone)) {
            LOGGER.info("checkPhone invalid phone is {}", phone);
            return false;
        }

        return true;
    }


    public static void main(String[] args) {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        HashMap<String, Integer> map2 = new HashMap<String, Integer>();
        List<SameDeviceBean> sameDeviceBeanList = new ArrayList<SameDeviceBean>();
        List<SameIpBean> sameIpBeanList = new ArrayList<SameIpBean>();
        MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean = new MemberDeviceAndApplyAndOrderBean();
        queryDirectRelationDataByPhoneNo("18516216801", getODataBaseDocumentTx(), map, map2,  sameDeviceBeanList,  sameIpBeanList,
                 memberDeviceAndApplyAndOrderBean);
    }

    /**
     * 查询具体业务指标
     * @param memberRelatedPhoneNo
     * @param tx
     * @param map
     * @param map2
     * @param sameDeviceBeanList
     * @param sameIpBeanList
     * @param memberDeviceAndApplyAndOrderBean
     * @return
     */
    private static long queryDirectRelationDataByPhoneNo(String memberRelatedPhoneNo, ODatabaseDocumentTx tx, Map<String, Integer> map, Map<String, Integer> map2,
                                                         List<SameDeviceBean> sameDeviceBeanList, List<SameIpBean> sameIpBeanList,
                                                         MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean) {
        OResultSet phoneInfos = tx.command(new OCommandSQL("select @rid as phoneRid0, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone = ?")).execute(new Object[]{memberRelatedPhoneNo});
        ODocument phoneInfo = ((ODocument) phoneInfos.get(0));
        ODocument phoneRecord0 = phoneInfo.field("phoneRid0");
        ORecordLazyList members0 = phoneInfo.field("members0");
//        long memberId = ((ODocument) members0.get(0)).field("memberId");
        ORecordLazyList ocrs = phoneInfo.field("callTos");
        Map<String, String> tempMap = new HashMap<String, String>();
        List<String> directPhones = new ArrayList<String>();


        //连接不同设备的个数
        int diffDeviceCount = 0;
        if (members0 != null && !members0.isEmpty()) {
            ODocument member = (ODocument) members0.get(0);
            ORidBag in_HasDevice = member.field("out_MemberHasDevice");
            if (null != in_HasDevice && !in_HasDevice.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasDevice.iterator();
                while (it.hasNext()) {
                    diffDeviceCount++;

                    SameDeviceBean sameDeviceBean = new SameDeviceBean();
                    OIdentifiable t = it.next();
                    ODocument inDevice = (ODocument) t;
                    ODocument device = inDevice.field("in");
                    ORidBag out_HasDevice = device.field("in_MemberHasDevice");
                    String deviceId = device.field("deviceId");
                    sameDeviceBean.setDeviceId(deviceId);
                    //同设备客户个数
                    int sameDeviceCount = 0;
                    if (null != out_HasDevice && !out_HasDevice.isEmpty()) {
                        Iterator<OIdentifiable> it1 = out_HasDevice.iterator();
                        while (it1.hasNext()) {
                            it1.next();
                            sameDeviceCount++;
                        }
                    }
                    sameDeviceBean.setDirect(sameDeviceCount - 1);
                    sameDeviceBeanList.add(sameDeviceBean);
                }
            }
        }

        //连接不同ip的个数
        int diffIpCount = 0;
        if (members0 != null && !members0.isEmpty()) {
            ODocument member = (ODocument) members0.get(0);
            ORidBag in_HasIp = member.field("out_MemberHasIp");
            if (null != in_HasIp && !in_HasIp.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasIp.iterator();
                while (it.hasNext()) {
                    it.next();
                    diffIpCount++;

                    SameIpBean sameIpBean = new SameIpBean();
                    OIdentifiable t = it.next();
                    ODocument inIp = (ODocument) t;
                    ODocument ip1 = inIp.field("in");
                    ORidBag out_HasIp = ip1.field("in_MemberHasIp");
                    String ip = ip1.field("ip");
                    sameIpBean.setIp(ip);
                    //同ip的客户个数
                    int sameIpCount = 0;
                    if (null != out_HasIp && !out_HasIp.isEmpty()) {
                        Iterator<OIdentifiable> it1 = out_HasIp.iterator();
                        while (it1.hasNext()) {
                            it1.next();
                            sameIpCount++;
                        }
                    }
                    sameIpBean.setDirect(sameIpCount - 1);
                    sameIpBeanList.add(sameIpBean);

                }
            }
        }

        //连接不同申请件数
        int diffApplyCount = 0;
        // apply连接不同商户个数
        int diffMerchantCount = 0;
        if (members0 != null && !members0.isEmpty()) {
            ODocument member = (ODocument) members0.get(0);
            ORidBag in_HasApply = member.field("out_MemberHasApply");
            if (null != in_HasApply && !in_HasApply.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasApply.iterator();
                while (it.hasNext()) {
                    diffApplyCount++;
                    OIdentifiable t = it.next();
                    ODocument inApply = (ODocument) t;
                    ODocument apply = inApply.field("in");
                    ORidBag in_HasStore = apply.field("out_ApplyHasStore");
                    if (null != in_HasStore && !in_HasStore.isEmpty()) {
                        Iterator<OIdentifiable> it1 = in_HasStore.iterator();
                        while (it1.hasNext()) {
                            it1.next();
                            diffMerchantCount++;
                        }
                    }
                }
            }
        }
        //连接不同订单数
        int diffOrderCount = 0;
        if (members0 != null && !members0.isEmpty()) {
            ODocument member = (ODocument) members0.get(0);
            ORidBag in_HasOrder = member.field("out_MemberHasOrder");
            if (null != in_HasOrder && !in_HasOrder.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasOrder.iterator();
                while (it.hasNext()) {
                    it.next();
                    diffOrderCount++;
                }
            }
        }

        memberDeviceAndApplyAndOrderBean.setHasDeviceNum(diffDeviceCount);
        memberDeviceAndApplyAndOrderBean.setHasIpNum(diffIpCount);
        memberDeviceAndApplyAndOrderBean.setHasApplNum(diffApplyCount);
        memberDeviceAndApplyAndOrderBean.setHasMerchantNum(diffMerchantCount);
        memberDeviceAndApplyAndOrderBean.setHasOrderNum(diffOrderCount);

        //一度联系人过件个数
        int contactAccept = 0;
        //一度联系人拒件个数
        int contactRefuse = 0;
        //一度联系人逾期个数
        int contactOverdue = 0;
        //一度联系人黑名单个数
        int contactBlack = 0;

        if (ocrs != null && !ocrs.isEmpty()) {
            int ocrSize = ocrs.size();
            LOGGER.info("queryDirectRelationDataByPhoneNo ocrSize is {}, memberRelatedPhoneNo is {}", ocrSize, memberRelatedPhoneNo);
            for (int j = 0; j < ocrSize; j++) {
                ODocument ocr = (ODocument) ocrs.get(j);
                ODocument tempPhoneRecordIn1 = ocr.field("in");//callTo边
                ODocument tempPhoneRecordOut1 = ocr.field("out");
                //设置一级联系人的record
                ODocument phoneRecord1 = getRid(tempPhoneRecordIn1).equals(getRid(phoneRecord0)) ? tempPhoneRecordOut1 : tempPhoneRecordIn1;//phone点

                String phone = phoneRecord1.field("phone").toString();

                //对一度联系人phone做下校验
                if (directPhones.contains(phone) || memberRelatedPhoneNo.equals(phone)) {
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
                            if (!memberRelatedPhoneNo.equals(indirectphone)) {
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
                            if (!memberRelatedPhoneNo.equals(indirectphone)) {
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

                //查询过件、拒件、逾期
                ORidBag in_HasPhone = phoneRecord1.field("in_HasPhone");
                if (null != in_HasPhone && !in_HasPhone.isEmpty()) {
                    Iterator<OIdentifiable> it = in_HasPhone.iterator();
                    while (it.hasNext()) {
                        OIdentifiable t = it.next();
                        ODocument member = (ODocument) t;
                        ODocument member1 = member.field("out");
                        Boolean isBlack = member1.field("isBlack");
                        Boolean isOverdue = member1.field("isOverdue");
                        if (isBlack) {
                            contactBlack++;
                        }
                        if (isOverdue) {
                            contactOverdue++;
                        }
                        ORidBag out_MemberHasOrder = member1.field("out_MemberHasOrder");
                        if (null != out_MemberHasOrder && !out_MemberHasOrder.isEmpty()) {
                            long lastTime = 0;
                            Iterator<OIdentifiable> it1 = out_MemberHasOrder.iterator();
                            String originalStatus = null;
                            while (it1.hasNext()) {
                                OIdentifiable t1 = it1.next();
                                ODocument order = (ODocument) t1;
                                ODocument order1 = order.field("in");
                                Date createdDatetime = order1.field("createdDatetime");
                                long stringToLong = 0;
                                stringToLong = DateUtils.dateToLong(createdDatetime);
                                if (stringToLong > lastTime) {
                                    lastTime = stringToLong;
                                    originalStatus = order1.field("originalStatus");
                                }
                            }
                            if (ConstantHelper.REFUSE_APPLY_FLAG.equals(originalStatus)) {
                                contactRefuse++;
                            } else if (ConstantHelper.PASS_APPLY_FLAG.equals(originalStatus)) {
                                contactAccept++;
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

            if (tempSet != null) {
                tempSet.clear();
                tempSet = null;
            }
        }

        memberDeviceAndApplyAndOrderBean.setContactBlackMemberNum(contactBlack);
        memberDeviceAndApplyAndOrderBean.setContactOverdueMemberNum(contactOverdue);
        memberDeviceAndApplyAndOrderBean.setContactAcceptMemberNum(contactAccept);
        memberDeviceAndApplyAndOrderBean.setContactRefuseMemberNum(contactRefuse);


        if (tempMap != null) {
            tempMap.clear();
            tempMap = null;
        }
        if (directPhones != null) {
            directPhones.clear();
            directPhones = null;
        }
        if (ocrs != null) {
            ocrs.clear();
            ocrs = null;
        }

        return 0;
    }



    /**
     * 构建结构化数据入mysql指标数据库
     * @param memberAndPhoneBean
     * @param tx
     */
    private static void dealBasicDataByPhone(MemberAndPhoneBean memberAndPhoneBean, ODatabaseDocumentTx tx) {
//        String sql = "select in('PhoneHasApply').phone as MemberRelatedPhone, out('ApplyHasOrder').orderNo as RelatedOrderNo from Apply where applyNo = ?";
//        DirectRelationDataBean directRelationDataBean = queryDirectRelationDataByNo(applyNo, tx, sql);
//        LOGGER.info("directRelationDataBean is " + directRelationDataBean.toString());
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        HashMap<String, Integer> map2 = new HashMap<String, Integer>();

        List<SameDeviceBean> sameDeviceBeanList = new ArrayList<SameDeviceBean>();
        List<SameIpBean> sameIpBeanList = new ArrayList<SameIpBean>();
        MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean = new MemberDeviceAndApplyAndOrderBean();

        String phone = memberAndPhoneBean.getPhones();

        queryDirectRelationDataByPhoneNo(phone, tx, map, map2,sameDeviceBeanList,sameIpBeanList,memberDeviceAndApplyAndOrderBean);

        //插入一度和二度联系人指标开始
        List<IndexData> indexDatas = new ArrayList<IndexData>();
        Map<String, Integer> directResultMap = map;
        Set<Map.Entry<String, Integer>> directSet = directResultMap.entrySet();
        List<String> directMarks = new ArrayList<String>();

        List<String> onlyAppNos = memberAndPhoneBean.getOnlyAppNos();
        List<String> onlyOrderNos = memberAndPhoneBean.getOnlyOrderNos();
        List<ApplyRelateOrder> applyRelateOrderNos = memberAndPhoneBean.getApplyRelateOrderNos();

        for (Map.Entry<String, Integer> en : directSet) {

            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                            IndexNameEnum.fromValue(en.getKey()),en.getValue(),0,"",null,null);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                            IndexNameEnum.fromValue(en.getKey()),en.getValue(),0,"",null,null);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                            IndexNameEnum.fromValue(en.getKey()),en.getValue(),0,"",null,null);
                }
            }
            directMarks.add(en.getKey());
        }


        Map<String, Integer> indirectResultMap = map2;
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
                if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                    int onlyAppNosSize = onlyAppNos.size();
                    for (int i = 0; i < onlyAppNosSize; i++) {
                        AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                                onlyAppNos.get(i),null,
                                IndexNameEnum.fromValue(en.getKey()),0,en.getValue(),"",null,null);
                    }
                }

                if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                    int onlyOrderNosSize = onlyOrderNos.size();
                    for (int i = 0; i < onlyOrderNosSize; i++) {
                        AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                                null,onlyOrderNos.get(i),
                                IndexNameEnum.fromValue(en.getKey()),0,en.getValue(),"",null,null);
                    }
                }

                if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                    int applyRelateOrderNosSize = applyRelateOrderNos.size();
                    for (int i = 0; i < applyRelateOrderNosSize; i++) {
                        AddIndexDatas(indexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                                applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                                IndexNameEnum.fromValue(en.getKey()),0,en.getValue(),"",null,null);
                    }
                }

            }
        }

        LOGGER.info("dealBasicDataByPhone insertPhonetagIndex");
        insertPhonetagIndex(indexDatas);
        //插入一度和二度联系人指标结束

        //插入同设备客户个数指标开始
        int sameDeviceListSize = sameDeviceBeanList.size();
        List<IndexData> deviceIndexDataList = new ArrayList<IndexData>();
        for (int j = 0; j < sameDeviceListSize; j++){
            String deviceId = sameDeviceBeanList.get(j).getDeviceId();
            int direct = sameDeviceBeanList.get(j).getDirect();
            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                            "equal_device_member_num",direct,0,"",deviceId,null);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                            "equal_device_member_num",direct,0,"",deviceId,null);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                            "equal_device_member_num",direct,0,"",deviceId,null);
                }
            }
        }
        //插入同设备客户个数指标结束

        //插入同ip的客户个数指标开始
        int sameIpListSize = sameIpBeanList.size();
        List<IndexData> ipIndexDataList = new ArrayList<IndexData>();
        for (int j = 0; j < sameIpListSize; j++){
            String ip = sameIpBeanList.get(j).getIp();
            int direct = sameIpBeanList.get(j).getDirect();
            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(ipIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                            "equal_ip_member_num",direct,0,"",null,ip);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(ipIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                            "equal_ip_member_num",direct,0,"",null,ip);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(ipIndexDataList,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                            "equal_ip_member_num",direct,0,"",null,ip);
                }
            }
        }
        LOGGER.info("dealBasicDataByPhone insertDeviceAndIpIndex");
        insertDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList);
        //插入同ip的客户个数指标结束


        //插入会员指标开始
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
            int onlyAppNosSize = onlyAppNos.size();
            for (int i = 0; i < onlyAppNosSize; i++) {

                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "contact_accept_member_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "contact_refuse_member_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "contact_overdue_member_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),onlyAppNos.get(i),null,
                        "contact_black_member_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(),0,"",null,null);
            }
        }

        if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
            int onlyOrderNosSize = onlyOrderNos.size();
            for (int i = 0; i < onlyOrderNosSize; i++) {
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "has_device_num",memberDeviceAndApplyAndOrderBean.getHasDeviceNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "has_ip_num",memberDeviceAndApplyAndOrderBean.getHasIpNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "has_merchant_num",memberDeviceAndApplyAndOrderBean.getHasMerchantNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "has_appl_num",memberDeviceAndApplyAndOrderBean.getHasApplNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "has_order_num",memberDeviceAndApplyAndOrderBean.getHasOrderNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "contact_accept_member_num",memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "contact_refuse_member_num",memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "contact_overdue_member_num",memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),null,onlyOrderNos.get(i),
                        "contact_black_member_num",memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(),0,"",null,null);
            }
        }

        if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
            int applyRelateOrderNosSize = applyRelateOrderNos.size();
            for (int i = 0; i < applyRelateOrderNosSize; i++) {
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "has_device_num",memberDeviceAndApplyAndOrderBean.getHasDeviceNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "has_ip_num",memberDeviceAndApplyAndOrderBean.getHasIpNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "has_merchant_num",memberDeviceAndApplyAndOrderBean.getHasMerchantNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "has_appl_num",memberDeviceAndApplyAndOrderBean.getHasApplNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "has_order_num",memberDeviceAndApplyAndOrderBean.getHasOrderNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "contact_accept_member_num",memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "contact_refuse_member_num",memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "contact_overdue_member_num",memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(),0,"",null,null);
                AddIndexDatas(memberIndexDatas,Long.valueOf(memberAndPhoneBean.getMemberId()),memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(),applyRelateOrderNos.get(i).getOrder(),
                        "contact_black_member_num",memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(),0,"",null,null);
            }
        }


        LOGGER.info("dealBasicDataByPhone insertMemberIndex");
        insertMemberIndex(memberIndexDatas);

        //插入会员指标结束

        if (onlyAppNos != null) {
            onlyAppNos.clear();
            onlyAppNos = null;
        }

        if (onlyOrderNos != null) {
            onlyOrderNos.clear();
            onlyOrderNos = null;
        }

        if (applyRelateOrderNos != null) {
            applyRelateOrderNos.clear();
            applyRelateOrderNos = null;
        }

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

    private static void AddIndexDatas(List<IndexData> indexDatas,long memberId,String mobile,String applyNo,String orderNo,String indexName,
            long direct,long indirect,String createTime,String deviceId,String ip){
        IndexData indexData = new IndexData();
        indexData.setMemberId(memberId);
        indexData.setMobile(mobile);
        indexData.setDeviceId(deviceId);
        indexData.setIp(ip);
        indexData.setDirect(direct);
        indexData.setIndirect(indirect);
        indexData.setApplyNo(applyNo);
        indexData.setOrderNo(orderNo);
        indexData.setIndexName(indexName);
        indexDatas.add(indexData);
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

    public static void getBasicData(List<MemberAndPhoneBean> MemberAndPhoneBeanList, boolean isAllData) {
        LOGGER.info("getBasicData size" + MemberAndPhoneBeanList.size());
        if (isAllData) {
            if (null != MemberAndPhoneBeanList && MemberAndPhoneBeanList.size() > 0) {
                ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                dealAllBasicDataByApplyList(MemberAndPhoneBeanList, tx);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }
            }
        } else {

        }

    }

    private static void dealAllBasicDataByApplyList(List<MemberAndPhoneBean> memberAndPhoneBeanList, ODatabaseDocumentTx tx) {
        int size = memberAndPhoneBeanList.size();
        LOGGER.info("dealAllBasicDataByApplyList size {}", size);
        for (int i = 0; i < size; i++) {
            if (i % 1000 == 0) {
                LOGGER.info("dealAllBasicDataByApplyList i is {}", i);
            }
            dealBasicDataByPhone(memberAndPhoneBeanList.get(i), tx);
        }
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
        int memberAndPhoneCount = 0;
        Set<String> memberInfoMapSet = new HashSet<String>();
        Map<String, List<String>> memberInfoOnlyApplyMap = new HashMap<String, List<String>>();
        Map<String, List<ApplyRelateOrder>> memberInfoApplyRelateOrderMap = new HashMap<String, List<ApplyRelateOrder>>();
        Map<String, List<String>> memberInfoOnlyOrderMap = new HashMap<String, List<String>>();
        try {
            //查询总数
            if (!isAllDataQueryFlag) {
//                pstmt = mysqlBusinesConn.prepareStatement("SELECT count(1) as total FROM apply_info where DATE_FORMAT(created_datetime,'%Y-%m-%d') = ?");
//                pstmt.setString(1, date);
            } else {
                List<String> tempOrderList = new ArrayList<String>();
                //这个sql查询的是只有申请的用户
                pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no as apply_no,member_id as member_id,cellphone as phone FROM apply_info where order_no is null limit 100");
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    String applyNo = rs.getString("apply_no");
                    String memberId = rs.getString("member_id");
                    String phone = rs.getString("phone");
                    String memberInfoMapKey = (new StringBuilder(memberId).append(",").append(phone)).toString();
                    if (memberInfoOnlyApplyMap.containsKey(memberInfoMapKey)) {
                        memberInfoOnlyApplyMap.get(memberInfoMapKey).add(applyNo);
                    } else {
                        List<String> list = new ArrayList<String>();
                        list.add(applyNo);
                        memberInfoOnlyApplyMap.put(memberInfoMapKey, list);
                    }
                }

                //这个sql查询的是有申请关联订单的用户
                pstmt = mysqlBusinesConn.prepareStatement("SELECT apply_no as apply_no,member_id as member_id,cellphone as phone, order_no as order_no FROM apply_info where order_no is not null limit 100");
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    String applyNo = rs.getString("apply_no");
                    String orderNo = rs.getString("order_no");
                    tempOrderList.add(orderNo);
                    ApplyRelateOrder applyRelateOrder = new ApplyRelateOrder();
                    applyRelateOrder.setApply(applyNo);
                    applyRelateOrder.setOrder(orderNo);
                    String memberId = rs.getString("member_id");
                    String phone = rs.getString("phone");
                    String memberInfoMapKey = (new StringBuilder(memberId).append(",").append(phone)).toString();
                    if (memberInfoApplyRelateOrderMap.containsKey(memberInfoMapKey)) {
                        memberInfoApplyRelateOrderMap.get(memberInfoMapKey).add(applyRelateOrder);
                    } else {
                        List<ApplyRelateOrder> list = new ArrayList<ApplyRelateOrder>();
                        list.add(applyRelateOrder);
                        memberInfoApplyRelateOrderMap.put(memberInfoMapKey, list);
                    }
                }
                //这个sql查询的是有订单去除有申请的用户
                pstmt = mysqlBusinesConn.prepareStatement("SELECT order_no as order_no, member_id as member_id, mobile as phone FROM money_box_order limit 100");
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    String orderNo = rs.getString("order_no");
                    LOGGER.info("orderNo is {}", orderNo);
                    if (!tempOrderList.contains(orderNo)) {
                        String memberId = rs.getString("member_id");
                        String phone = rs.getString("phone");
                        String memberInfoMapKey = (new StringBuilder(memberId).append(",").append(phone)).toString();
                        if (memberInfoOnlyOrderMap.containsKey(memberInfoMapKey)) {
                            memberInfoOnlyOrderMap.get(memberInfoMapKey).add(orderNo);
                        } else {
                            List<String> list = new ArrayList<String>();
                            list.add(orderNo);
                            memberInfoOnlyOrderMap.put(memberInfoMapKey, list);
                        }
                    }
                }
                LOGGER.info("tempOrderList is {}", tempOrderList.toString());
                if (tempOrderList != null) {
                    tempOrderList.clear();
                    tempOrderList = null;
                }

                Set<String> memberInfoOnlyApplyMapSet = memberInfoOnlyApplyMap.keySet();
                Set<String> memberInfoApplyRelateOrderMapSet = memberInfoApplyRelateOrderMap.keySet();
                Set<String> memberInfoOnlyOrderMapSet = memberInfoOnlyOrderMap.keySet();
                memberInfoMapSet.addAll(memberInfoOnlyApplyMapSet);
                memberInfoMapSet.addAll(memberInfoApplyRelateOrderMapSet);
                memberInfoMapSet.addAll(memberInfoOnlyOrderMapSet);
                memberAndPhoneCount = memberInfoMapSet.size();
            }

            int allNum = Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"));
            int applimitNum = (memberAndPhoneCount % allNum == 0) ? memberAndPhoneCount / allNum : (memberAndPhoneCount / allNum + 1);
            int count = 0;
            Iterator<String> it = memberInfoMapSet.iterator();
            ArrayList<MemberAndPhoneBean> memberAndPhoneBeanArrayList = new ArrayList<MemberAndPhoneBean>();
            while (it.hasNext()) {
                count++;
                MemberAndPhoneBean memberAndPhoneBean = new MemberAndPhoneBean();
                String memberAndPhone = it.next();
                memberAndPhoneBean.setOnlyAppNos(memberInfoOnlyApplyMap.get(memberAndPhone));
                memberAndPhoneBean.setOnlyOrderNos(memberInfoOnlyOrderMap.get(memberAndPhone));
                memberAndPhoneBean.setApplyRelateOrderNos(memberInfoApplyRelateOrderMap.get(memberAndPhone));
                memberAndPhoneBean.setMemberId(memberAndPhone.split(",")[0]);
                memberAndPhoneBean.setPhones(memberAndPhone.split(",")[1]);
                memberAndPhoneBeanArrayList.add(memberAndPhoneBean);
                if (count == applimitNum || (!it.hasNext())) {
                    count = 0;
                    LOGGER.info("memberAndPhoneBeanArrayList is {}, count is {}", memberAndPhoneBeanArrayList.toString(), count);
                    BasicDataBatchTask basicDataBatchTask = new BasicDataBatchTask();
                    basicDataBatchTask.setAllData(isAllDataQueryFlag);
                    basicDataBatchTask.setMemberAndPhoneBeanList(memberAndPhoneBeanArrayList);
                    memberAndPhoneBeanArrayList.clear();
                    es.submit(basicDataBatchTask);
                }
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
            if (memberInfoOnlyApplyMap != null) {
                for (Map.Entry<String, List<String>> entry : memberInfoOnlyApplyMap.entrySet()) {
                    entry.getValue().clear();
                }
                memberInfoOnlyApplyMap.clear();
            }
            if (memberInfoOnlyOrderMap != null) {
                for (Map.Entry<String, List<String>> entry : memberInfoOnlyOrderMap.entrySet()) {
                    entry.getValue().clear();
                }
                memberInfoOnlyApplyMap.clear();
            }
            if (memberInfoApplyRelateOrderMap != null) {
                for (Map.Entry<String, List<ApplyRelateOrder>> entry : memberInfoApplyRelateOrderMap.entrySet()) {
                    entry.getValue().clear();
                }
                memberInfoOnlyApplyMap.clear();
            }
            if (memberAndPhoneBeanArrayList != null) {
                memberAndPhoneBeanArrayList.clear();
            }

            if (memberInfoMapSet != null) {
                memberInfoMapSet.clear();
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
