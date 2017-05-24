package cn.memedai.orientdb.fraud.statistics.utils;

import cn.memedai.orientdb.fraud.statistics.ConstantHelper;
import cn.memedai.orientdb.fraud.statistics.bean.*;
import cn.memedai.orientdb.fraud.statistics.entity.IndexData;
import cn.memedai.orientdb.fraud.statistics.entity.IndexNameEnum;
import cn.memedai.orientdb.fraud.statistics.task.BasicDataBatchTask;
import cn.memedai.orientdb.fraud.statistics.task.HanlderThreadFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by hangyu on 2017/5/10.
 */
public class SqlUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

  /*  public static Map<String, List<ApplyRelateOrder>> memberInfoOnlyApplyMap = new HashMap<String, List<ApplyRelateOrder>>();
    public static Map<String, List<ApplyRelateOrder>> memberInfoApplyRelateOrderMap = new HashMap<String, List<ApplyRelateOrder>>();
    public static Map<String, List<ApplyRelateOrder>> memberInfoOnlyOrderMap = new HashMap<String, List<ApplyRelateOrder>>();*/

    public static Map<String, Integer> deviceHasMemeberMap = new HashMap<String, Integer>();
    public static Map<Long, List<String>> memberHasDeviceMap = new HashMap<Long, List<String>>();
    public static Map<String, Integer> ipHasMemeberMap = new HashMap<String, Integer>();
    public static Map<Long, List<String>> memberHasIpMap = new HashMap<Long, List<String>>();

    public static Map<Long, Set<String>> memberHasStoreMap = new HashMap<Long, Set<String>>();

    //会员最近一个订单的状态
    public static Map<Long, String> memberHasLastOrderStatus = new HashMap<Long, String>();
    public static long memberId = 0;
    public static Lock lock = new ReentrantLock();

    //一度联系人包含的而度联系人集合
    public static Map<String, Map<String, String>> phoneHasIndirectMap = new HashMap<String, Map<String, String>>();

    private static ODatabaseDocumentTx getODataBaseDocumentTx() {
        ODatabaseDocumentTx tx = null;
        try {
            tx = new ODatabaseDocumentTx(ConfigUtils.getProperty("orientDbUrl")).open(ConfigUtils.getProperty("orientDbUserName"), ConfigUtils.getProperty("orientDbUserPassword"));
        } catch (Exception e) {
            LOGGER.error("getODataBaseDocumentTx has e is {}", e);
        }
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
            return false;
        }

        if (phone.length() < ConstantHelper.BUSINESS_PHONE_LENGTH) {
            return false;
        }

        if (phone.length() >= 2) {
            if (ConstantHelper.BUSINESS_PHONE_1.equals(phone.substring(0, 1))) {
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_2.equals(phone.substring(0, 1))) {
                return false;
            }
        }

        if (phone.length() >= 3) {
            if (ConstantHelper.BUSINESS_PHONE_3.equals(phone.substring(0, 2))) {
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_4.equals(phone.substring(0, 2))) {
                return false;
            }
        }

        if (phone.length() >= 5) {
            if (ConstantHelper.BUSINESS_PHONE_5.equals(phone.substring(0, 4))) {
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_6.equals(phone.substring(0, 4))) {
                return false;
            }
            if (ConstantHelper.BUSINESS_PHONE_7.equals(phone.substring(0, 4))) {
                return false;
            }
        }

        if (ConstantHelper.BUSINESS_PHONE_7.equals(phone)) {
            return false;
        }

        return true;
    }


    /**
     * 查询具体业务指标
     *
     * @param memberRelatedPhoneNo
     * @param map
     * @param map2
     * @param memberDeviceAndApplyAndOrderBean
     * @return
     */
    private static long queryDirectRelationDataByPhoneNo(String memberRelatedPhoneNo, Map<String, Integer> map, Map<String, Integer> map2,
                                                         MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean, ODocument phoneInfo,
                                                         List<SameDeviceBean> sameDeviceBeanList,List<SameIpBean> sameIpBeanList,Boolean isAllData) {
        //OResultSet phoneInfos = tx.command(new OCommandSQL("select @rid as phoneRid0, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone = ?")).execute(new Object[]{memberRelatedPhoneNo});
        //ODocument phoneInfo = ((ODocument) phoneInfos.get(0));
        ODocument phoneRecord0 = phoneInfo.field("phoneRid0");
        ORecordLazyList members0 = phoneInfo.field("members0");
        ORecordLazyList ocrs = phoneInfo.field("callTos");
        Map<String, String> tempMap = new HashMap<String, String>();
        List<String> directPhones = new ArrayList<String>();
        //LOGGER.info("queryDirectRelationDataByPhoneNo memberRelatedPhoneNo is {}", memberRelatedPhoneNo);
        //连接不同设备的个数

        if (members0 != null && !members0.isEmpty()) {
            int diffDeviceCount = 0;
            ODocument member = (ODocument) members0.get(0);
            ORidBag in_HasDevice = member.field("out_MemberHasDevice");
            if (null != in_HasDevice && !in_HasDevice.isEmpty()) {
                diffDeviceCount = in_HasDevice.size();
                if (!isAllData){
                    Iterator<OIdentifiable> it = in_HasDevice.iterator();
                    while (it.hasNext()) {
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
                            sameDeviceCount = out_HasDevice.size();
                        }
                        sameDeviceBean.setDirect(sameDeviceCount);
                        sameDeviceBeanList.add(sameDeviceBean);
                    }
                }
            }

            //连接不同ip的个数
            int diffIpCount = 0;
            ORidBag in_HasIp = member.field("out_MemberHasIp");
            if (null != in_HasIp && !in_HasIp.isEmpty()) {
                diffIpCount = in_HasIp.size();
                if (!isAllData) {
                    Iterator<OIdentifiable> it = in_HasIp.iterator();
                    while (it.hasNext()) {
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
                            sameIpCount = out_HasIp.size();
                        }
                        sameIpBean.setDirect(sameIpCount);
                        sameIpBeanList.add(sameIpBean);

                    }
                }
            }

            //连接不同申请件数
            int diffApplyCount = 0;
            ORidBag in_HasApply = member.field("out_MemberHasApply");
            if (null != in_HasApply && !in_HasApply.isEmpty()) {
                diffApplyCount = in_HasApply.size();
            }

            //连接不同订单数
            int diffOrderCount = 0;
            ORidBag in_HasOrder = member.field("out_MemberHasOrder");

            if (null != in_HasOrder && !in_HasOrder.isEmpty()) {
                diffOrderCount = in_HasOrder.size();
            }


            // 连接不同商户个数
            int diffMerchantCount = 0;
            long memberId = member.field("memberId");

            Set<String> set = memberHasStoreMap.get(memberId);
            if (null != set) {
                diffMerchantCount = set.size();
            }

            memberDeviceAndApplyAndOrderBean.setHasDeviceNum(diffDeviceCount);
            memberDeviceAndApplyAndOrderBean.setHasIpNum(diffIpCount);
            memberDeviceAndApplyAndOrderBean.setHasApplNum(diffApplyCount);
            memberDeviceAndApplyAndOrderBean.setHasMerchantNum(diffMerchantCount);
            memberDeviceAndApplyAndOrderBean.setHasOrderNum(diffOrderCount);
        }


        //一度联系人过件个数
        int contactAccept = 0;
        //一度联系人拒件个数
        int contactRefuse = 0;
        //一度联系人逾期个数
        int contactOverdue = 0;
        //一度联系人黑名单个数
        int contactBlack = 0;

        Map<String, String> hasdirectMap = new HashMap<String, String>();
        if (ocrs != null && !ocrs.isEmpty()) {
            int ocrSize = ocrs.size();
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
                    lock.lock();
                    Map<String, String> hasIndirectMap = null;
                    boolean flag = phoneHasIndirectMap.containsKey(phone);
                    if (flag) {
                        hasIndirectMap = phoneHasIndirectMap.get(phone);
                        if (null != hasIndirectMap && !hasIndirectMap.isEmpty()) {
                            for (Map.Entry<String, String> en : hasIndirectMap.entrySet()) {
                                tempMap.put(en.getKey(), en.getValue());
                            }
                        }
                    }

                    lock.unlock();

                    if (!flag) {
                        hasIndirectMap = new HashMap<String, String>();

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
                                            hasIndirectMap.put(indirectphone, mark);
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
                                            hasIndirectMap.put(indirectphone, mark);
                                        }
                                    }
                                }
                            }
                        }

                        lock.lock();
                        phoneHasIndirectMap.put(phone, hasIndirectMap);
                        lock.unlock();
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

                        hasdirectMap.put(phone, mark);

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

                        memberId = member1.field("memberId");

                        String originalStatus = null;

                        lock.lock();
                        originalStatus = memberHasLastOrderStatus.get(memberId);
                        lock.unlock();

                        if (null == originalStatus) {
                            ORidBag out_MemberHasOrder = member1.field("out_MemberHasOrder");
                            if (null != out_MemberHasOrder && !out_MemberHasOrder.isEmpty()) {
                                long lastTime = 0;
                                Iterator<OIdentifiable> it1 = out_MemberHasOrder.iterator();
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

                                lock.lock();
                                memberHasLastOrderStatus.put(memberId, originalStatus);
                                lock.unlock();

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

            lock.lock();
            phoneHasIndirectMap.put(memberRelatedPhoneNo, hasdirectMap);
            lock.unlock();

            if (null != tempMap) {
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
        //LOGGER.info("queryDirectRelationDataByPhoneNo end memberRelatedPhoneNo is {}", memberRelatedPhoneNo);
        return 0;
    }


    /**
     * 构建结构化数据入mysql指标数据库
     *
     * @param memberAndPhoneBean
     * @param tx
     */
    private static void dealBasicDataByPhone(MemberAndPhoneBean memberAndPhoneBean, ODatabaseDocumentTx tx, ODocument phoneInfo, boolean isAllData) throws RuntimeException {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        HashMap<String, Integer> map2 = new HashMap<String, Integer>();

        List<SameDeviceBean> sameDeviceBeanList = new ArrayList<SameDeviceBean>();
        List<SameIpBean> sameIpBeanList = new ArrayList<SameIpBean>();
        MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean = new MemberDeviceAndApplyAndOrderBean();
        String phone = memberAndPhoneBean.getPhones();
        Long memberId = Long.valueOf(memberAndPhoneBean.getMemberId());

        if (isAllData){
            List<String> deviceIdList = memberHasDeviceMap.get(memberId);
            if (deviceIdList != null && !deviceIdList.isEmpty()) {
                int deviceIdListSize = deviceIdList.size();
                for (int i = 0; i < deviceIdListSize; i++) {
                    SameDeviceBean sameDeviceBean = new SameDeviceBean();
                    sameDeviceBean.setDeviceId(deviceIdList.get(i));
                    sameDeviceBean.setDirect(deviceHasMemeberMap.get(deviceIdList.get(i)));
                    sameDeviceBeanList.add(sameDeviceBean);
                }
            }

            List<String> ipList = memberHasIpMap.get(memberId);
            if (ipList != null && !ipList.isEmpty()) {
                int ipListSize = ipList.size();
                for (int i = 0; i < ipListSize; i++) {
                    SameIpBean sameIpBean = new SameIpBean();
                    sameIpBean.setIp(ipList.get(i));
                    sameIpBean.setDirect(ipHasMemeberMap.get(ipList.get(i)));
                    sameIpBeanList.add(sameIpBean);
                }
            }
        }

        try {
            queryDirectRelationDataByPhoneNo(phone, map, map2, memberDeviceAndApplyAndOrderBean, phoneInfo,sameDeviceBeanList,sameIpBeanList,isAllData);
        } catch (Exception e) {
            LOGGER.error("dealBasicDataByPhone has e {}", e);
            throw new RuntimeException(e);
        }

        //插入一度和二度联系人指标开始
        List<IndexData> indexDatas = new ArrayList<IndexData>();
        Set<Map.Entry<String, Integer>> directSet = map.entrySet();
        List<String> directMarks = new ArrayList<String>();

        List<ApplyRelateOrder> onlyAppNos = memberAndPhoneBean.getOnlyAppNos();
        List<ApplyRelateOrder> onlyOrderNos = memberAndPhoneBean.getOnlyOrderNos();
        List<ApplyRelateOrder> applyRelateOrderNos = memberAndPhoneBean.getApplyRelateOrderNos();

        for (Map.Entry<String, Integer> en : directSet) {

            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            IndexNameEnum.fromValue(en.getKey()), en.getValue(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            IndexNameEnum.fromValue(en.getKey()), en.getValue(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            IndexNameEnum.fromValue(en.getKey()), en.getValue(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                }
            }
            directMarks.add(en.getKey());
        }

        //判断该标签是否包含一度数据
        Set<Map.Entry<String, Integer>> indirectResultSet = map2.entrySet();
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
                        AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                                onlyAppNos.get(i).getApply(), null,
                                IndexNameEnum.fromValue(en.getKey()), 0, en.getValue(), onlyAppNos.get(i).getCreateTime(), null, null);
                    }
                }

                if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                    int onlyOrderNosSize = onlyOrderNos.size();
                    for (int i = 0; i < onlyOrderNosSize; i++) {
                        AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                                null, onlyOrderNos.get(i).getOrder(),
                                IndexNameEnum.fromValue(en.getKey()), 0, en.getValue(), onlyOrderNos.get(i).getCreateTime(), null, null);
                    }
                }

                if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                    int applyRelateOrderNosSize = applyRelateOrderNos.size();
                    for (int i = 0; i < applyRelateOrderNosSize; i++) {
                        AddIndexDatas(indexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                                applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                                IndexNameEnum.fromValue(en.getKey()), 0, en.getValue(), applyRelateOrderNos.get(i).getCreateTime(), null, null);
                    }
                }

            }
        }

        if (!isAllData) {
            LOGGER.info("dealBasicDataByPhone insertPhonetagIndex");
            insertPhonetagIndex(indexDatas);
            //插入一度和二度联系人指标结束
        }


        //插入同设备客户个数指标开始
        int sameDeviceListSize = sameDeviceBeanList.size();
        List<IndexData> deviceIndexDataList = new ArrayList<IndexData>();
        for (int j = 0; j < sameDeviceListSize; j++) {
            String deviceId = sameDeviceBeanList.get(j).getDeviceId();
            int direct = sameDeviceBeanList.get(j).getDirect();
            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "equal_device_member_num", direct, 0, onlyAppNos.get(i).getCreateTime(), deviceId, null);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "equal_device_member_num", direct, 0, onlyOrderNos.get(i).getCreateTime(), deviceId, null);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "equal_device_member_num", direct, 0, applyRelateOrderNos.get(i).getCreateTime(), deviceId, null);
                }
            }
        }
        //插入同设备客户个数指标结束

        //插入同ip的客户个数指标开始
        int sameIpListSize = sameIpBeanList.size();
        List<IndexData> ipIndexDataList = new ArrayList<IndexData>();
        for (int j = 0; j < sameIpListSize; j++) {
            String ip = sameIpBeanList.get(j).getIp();
            int direct = sameIpBeanList.get(j).getDirect();
            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "equal_ip_member_num", direct, 0, onlyAppNos.get(i).getCreateTime(), null, ip);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "equal_ip_member_num", direct, 0, onlyOrderNos.get(i).getCreateTime(), null, ip);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "equal_ip_member_num", direct, 0, applyRelateOrderNos.get(i).getCreateTime(), null, ip);
                }
            }
        }

        if (!isAllData) {
            LOGGER.info("dealBasicDataByPhone insertDeviceAndIpIndex");
            insertDeviceAndIpIndex(deviceIndexDataList, ipIndexDataList);
            //插入同ip的客户个数指标结束
        }


        //插入会员指标开始
        List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
        if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
            int onlyAppNosSize = onlyAppNos.size();
            for (int i = 0; i < onlyAppNosSize; i++) {

                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "contact_accept_member_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "contact_refuse_member_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "contact_overdue_member_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                        "contact_black_member_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
            }
        }

        if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
            int onlyOrderNosSize = onlyOrderNos.size();
            for (int i = 0; i < onlyOrderNosSize; i++) {
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "contact_accept_member_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "contact_refuse_member_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "contact_overdue_member_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                        "contact_black_member_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
            }
        }

        if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
            int applyRelateOrderNosSize = applyRelateOrderNos.size();
            for (int i = 0; i < applyRelateOrderNosSize; i++) {
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "contact_accept_member_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "contact_refuse_member_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "contact_overdue_member_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                        applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                        "contact_black_member_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
            }
        }


        if (!isAllData) {
            LOGGER.info("dealBasicDataByPhone insertMemberIndex");
            insertMemberIndex(memberIndexDatas);
        }
        //插入会员指标结束

        if (isAllData) {
            lock.lock();
            exportToCsv(indexDatas, deviceIndexDataList, ipIndexDataList, memberIndexDatas);
            lock.unlock();
        }

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

        if (directSet != null) {
            directSet.clear();
            directSet = null;
        }
        if (indirectResultSet != null) {
            indirectResultSet.clear();
            indirectResultSet = null;
        }

        if (map != null) {
            map.clear();
            map = null;
        }

        if (map2 != null) {
            map2.clear();
            map2 = null;
        }

        if (memberIndexDatas != null) {
            memberIndexDatas.clear();
            memberIndexDatas = null;
        }

        if (ipIndexDataList != null) {
            ipIndexDataList.clear();
            ipIndexDataList = null;
        }

        if (deviceIndexDataList != null) {
            deviceIndexDataList.clear();
            deviceIndexDataList = null;
        }
        if (sameDeviceBeanList != null) {
            sameDeviceBeanList.clear();
            sameDeviceBeanList = null;
        }

        if (sameIpBeanList != null) {
            sameIpBeanList.clear();
            sameIpBeanList = null;
        }
    }

    private static void AddIndexDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
                                      long direct, long indirect, String createTime, String deviceId, String ip) {
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
        indexData.setCreateTime(createTime);
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

    public static void getBasicData(List<MemberAndPhoneBean> memberAndPhoneBeanList, boolean isAllData) {
        if (isAllData) {
            if (null != memberAndPhoneBeanList && memberAndPhoneBeanList.size() > 0) {
                ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                dealAllBasicDataByApplyList(memberAndPhoneBeanList, tx, isAllData);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }
            }
        }else{
            //分出MemberAndPhoneBeanList哪些是新增 哪些是修改
            List<MemberAndPhoneBean> addBeanList = new ArrayList<MemberAndPhoneBean>();
            List<MemberAndPhoneBean> updateBeanList = new ArrayList<MemberAndPhoneBean>();

            Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                    ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

            PreparedStatement pstmt = null;
            ResultSet rs = null;

            int memberAndPhoneBeanListSize = memberAndPhoneBeanList.size();
            String phone = null;
            String memberId = null;
            List<ApplyRelateOrder> onlyAppNos = new ArrayList<ApplyRelateOrder>();
            List<ApplyRelateOrder> onlyOrderNos = new ArrayList<ApplyRelateOrder>();
            List<ApplyRelateOrder> applyRelateOrderNos = new ArrayList<ApplyRelateOrder>();
            for (int i = 0; i < memberAndPhoneBeanListSize; i++){
                MemberAndPhoneBean addmemberAndPhoneBean = new MemberAndPhoneBean();
                addmemberAndPhoneBean.setPhones(memberAndPhoneBeanList.get(i).getPhones());
                addmemberAndPhoneBean.setMemberId(memberAndPhoneBeanList.get(i).getMemberId());

                MemberAndPhoneBean updatememberAndPhoneBean = new MemberAndPhoneBean();
                updatememberAndPhoneBean.setPhones(memberAndPhoneBeanList.get(i).getPhones());
                updatememberAndPhoneBean.setMemberId(memberAndPhoneBeanList.get(i).getMemberId());

                int num = 0;

                onlyAppNos = memberAndPhoneBeanList.get(i).getOnlyAppNos();
                int onlyAppNosSize = onlyAppNos.size();
                List<ApplyRelateOrder> addonlyAppNos = new ArrayList<ApplyRelateOrder>();
                List<ApplyRelateOrder> updateonlyAppNos = new ArrayList<ApplyRelateOrder>();
                for (int j = 0; j < onlyAppNosSize; j++){
                    try {
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `member_index` where apply_no = ?");
                        pstmt.setString(1,  onlyAppNos.get(j).getApply());
                        rs = pstmt.executeQuery();
                        while (rs.next()){
                            num = rs.getInt("num");
                        }
                        if (num > 0){
                            updateonlyAppNos.add(onlyAppNos.get(j));
                        }else{
                            addonlyAppNos.add(onlyAppNos.get(j));
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                onlyOrderNos = memberAndPhoneBeanList.get(i).getOnlyOrderNos();
                int onlyOrderNosSize = onlyOrderNos.size();
                List<ApplyRelateOrder> addonlyOrderNos = new ArrayList<ApplyRelateOrder>();
                List<ApplyRelateOrder> updateonlyOrderNos = new ArrayList<ApplyRelateOrder>();
                for (int j = 0; j < onlyOrderNosSize; j++){
                    try {
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `member_index` where order_no = ?");
                        pstmt.setString(1,  onlyOrderNos.get(j).getOrder());
                        rs = pstmt.executeQuery();
                        while (rs.next()){
                            num = rs.getInt("num");
                        }
                        if (num > 0){
                            updateonlyOrderNos.add(onlyOrderNos.get(j));
                        }else{
                            addonlyOrderNos.add(onlyOrderNos.get(j));
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                applyRelateOrderNos = memberAndPhoneBeanList.get(i).getApplyRelateOrderNos();
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                List<ApplyRelateOrder> addapplyRelateOrderNos = new ArrayList<ApplyRelateOrder>();
                List<ApplyRelateOrder> updateapplyRelateOrderNos = new ArrayList<ApplyRelateOrder>();
                for (int j = 0; j < applyRelateOrderNosSize; j++){
                    try {
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `member_index` where apply_no = ?");
                        pstmt.setString(1,  applyRelateOrderNos.get(j).getApply());
                        rs = pstmt.executeQuery();
                        while (rs.next()){
                            num = rs.getInt("num");
                        }
                        if (num > 0){
                            updateapplyRelateOrderNos.add(applyRelateOrderNos.get(j));
                        }else{
                            addapplyRelateOrderNos.add(applyRelateOrderNos.get(j));
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                addmemberAndPhoneBean.setOnlyAppNos(addonlyAppNos);
                addmemberAndPhoneBean.setOnlyOrderNos(addonlyOrderNos);
                addmemberAndPhoneBean.setApplyRelateOrderNos(addapplyRelateOrderNos);
                addBeanList.add(addmemberAndPhoneBean);

                updatememberAndPhoneBean.setOnlyAppNos(updateonlyAppNos);
                updatememberAndPhoneBean.setOnlyOrderNos(updateonlyOrderNos);
                updatememberAndPhoneBean.setApplyRelateOrderNos(updateapplyRelateOrderNos);
                updateBeanList.add(updatememberAndPhoneBean);
            }


            if (null != addBeanList && addBeanList.size() > 0) {
              /*  ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                dealAllBasicDataByApplyList(addBeanList, tx, isAllData);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }*/
            }


            if (null != updateBeanList && updateBeanList.size() > 0) {
                ODatabaseDocumentTx tx = getODataBaseDocumentTx();
                //TO_DO
                dealUpdateBasicDataByApplyList(updateBeanList, tx);
                if (tx != null) {
                    OrientDbUtils.close(tx);
                }
            }
        }
    }

    public static void main(String[] args) {
        List<String> s = new ArrayList<String>();
        s.add("15918193532");
        s.add("18739919550");
        ODatabaseDocumentTx tx = getODataBaseDocumentTx();
        String sql = "select @rid as phoneRid0,phone as phone, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone in " + s.toString();
        OResultSet phoneInfos = tx.command(new OCommandSQL(sql)).execute(new Object[]{});

    }

    /**
     * 增量需要修改的申请或订单
     * @param memberAndPhoneBeanList
     * @param tx
     */
    private static void dealUpdateBasicDataByApplyList(List<MemberAndPhoneBean> memberAndPhoneBeanList, ODatabaseDocumentTx tx){

        int memberAndPhoneBeanListSize = memberAndPhoneBeanList.size();

        for (int k = 0; k < memberAndPhoneBeanListSize; k++){
            MemberAndPhoneBean memberAndPhoneBean = memberAndPhoneBeanList.get(k);
            List<ApplyRelateOrder> onlyAppNos = memberAndPhoneBean.getOnlyAppNos();
            List<ApplyRelateOrder> onlyOrderNos = memberAndPhoneBean.getOnlyOrderNos();
            List<ApplyRelateOrder> applyRelateOrderNos = memberAndPhoneBean.getApplyRelateOrderNos();

            List<SameDeviceBean> sameDeviceBeanList = new ArrayList<SameDeviceBean>();
            List<SameIpBean> sameIpBeanList = new ArrayList<SameIpBean>();

            MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean = new MemberDeviceAndApplyAndOrderBean();
            String sql = "select @rid as member from member where memberId = ?";
            OResultSet members = tx.command(new OCommandSQL(sql)).execute(new Object[]{memberAndPhoneBean.getMemberId()});
            ODocument members1 = ((ODocument) members.get(0));
            ODocument member = members1.field("member");


            //连接不同设备的个数
            int diffDeviceCount = 0;
            ORidBag in_HasDevice = member.field("out_MemberHasDevice");
            if (null != in_HasDevice && !in_HasDevice.isEmpty()) {
                diffDeviceCount = in_HasDevice.size();
            }

            if (null != in_HasDevice && !in_HasDevice.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasDevice.iterator();
                while (it.hasNext()) {
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
                        sameDeviceCount = out_HasDevice.size();
                    }
                    sameDeviceBean.setDirect(sameDeviceCount);
                    sameDeviceBeanList.add(sameDeviceBean);
                }
            }

            //插入同设备客户个数指标开始
            int sameDeviceListSize = sameDeviceBeanList.size();
            List<IndexData> deviceIndexDataList = new ArrayList<IndexData>();
            for (int j = 0; j < sameDeviceListSize; j++) {
                String deviceId = sameDeviceBeanList.get(j).getDeviceId();
                int direct = sameDeviceBeanList.get(j).getDirect();
                if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                    int onlyAppNosSize = onlyAppNos.size();
                    for (int i = 0; i < onlyAppNosSize; i++) {
                        AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                                "equal_device_member_num", direct, 0, onlyAppNos.get(i).getCreateTime(), deviceId, null);
                    }
                }

                if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                    int onlyOrderNosSize = onlyOrderNos.size();
                    for (int i = 0; i < onlyOrderNosSize; i++) {
                        AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                                "equal_device_member_num", direct, 0, onlyOrderNos.get(i).getCreateTime(), deviceId, null);
                    }
                }

                if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                    int applyRelateOrderNosSize = applyRelateOrderNos.size();
                    for (int i = 0; i < applyRelateOrderNosSize; i++) {
                        AddIndexDatas(deviceIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                                applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                                "equal_device_member_num", direct, 0, applyRelateOrderNos.get(i).getCreateTime(), deviceId, null);
                    }
                }
            }
            //插入同设备客户个数指标结束

            updateDeviceIndex(deviceIndexDataList);

            //连接不同ip的个数
            int diffIpCount = 0;
            ORidBag in_HasIp = member.field("out_MemberHasIp");
            if (null != in_HasIp && !in_HasIp.isEmpty()) {
                diffIpCount = in_HasIp.size();
            }
            if (null != in_HasIp && !in_HasIp.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasIp.iterator();
                while (it.hasNext()) {
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
                        sameIpCount = out_HasIp.size();
                    }
                    sameIpBean.setDirect(sameIpCount);
                    sameIpBeanList.add(sameIpBean);

                }
            }

            int sameIpListSize = sameIpBeanList.size();
            List<IndexData> ipIndexDataList = new ArrayList<IndexData>();
            for (int j = 0; j < sameIpListSize; j++) {
                String ip = sameIpBeanList.get(j).getIp();
                int direct = sameIpBeanList.get(j).getDirect();
                if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                    int onlyAppNosSize = onlyAppNos.size();
                    for (int i = 0; i < onlyAppNosSize; i++) {
                        AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                                "equal_ip_member_num", direct, 0, onlyAppNos.get(i).getCreateTime(), null, ip);
                    }
                }

                if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                    int onlyOrderNosSize = onlyOrderNos.size();
                    for (int i = 0; i < onlyOrderNosSize; i++) {
                        AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                                "equal_ip_member_num", direct, 0, onlyOrderNos.get(i).getCreateTime(), null, ip);
                    }
                }

                if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                    int applyRelateOrderNosSize = applyRelateOrderNos.size();
                    for (int i = 0; i < applyRelateOrderNosSize; i++) {
                        AddIndexDatas(ipIndexDataList, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                                applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                                "equal_ip_member_num", direct, 0, applyRelateOrderNos.get(i).getCreateTime(), null, ip);
                    }
                }
            }

            updateIpIndex(ipIndexDataList);



            //连接不同申请件数
            int diffApplyCount = 0;
            ORidBag in_HasApply = member.field("out_MemberHasApply");
            if (null != in_HasApply && !in_HasApply.isEmpty()) {
                diffApplyCount = in_HasApply.size();
            }
            //连接不同订单数
            int diffOrderCount = 0;
            ORidBag in_HasOrder = member.field("out_MemberHasOrder");
            if (null != in_HasOrder && !in_HasOrder.isEmpty()) {
                diffOrderCount = in_HasOrder.size();
            }
            // 连接不同商户个数
            int diffMerchantCount = 0;
            long memberId = member.field("memberId");

            Set<String> set = memberHasStoreMap.get(memberId);
            if (null != set) {
                diffMerchantCount = set.size();
            }

            memberDeviceAndApplyAndOrderBean.setHasDeviceNum(diffDeviceCount);
            memberDeviceAndApplyAndOrderBean.setHasIpNum(diffIpCount);
            memberDeviceAndApplyAndOrderBean.setHasApplNum(diffApplyCount);
            memberDeviceAndApplyAndOrderBean.setHasMerchantNum(diffMerchantCount);
            memberDeviceAndApplyAndOrderBean.setHasOrderNum(diffOrderCount);

            List<IndexData> memberIndexDatas = new ArrayList<IndexData>();
            if (null != onlyAppNos && !onlyAppNos.isEmpty()) {
                int onlyAppNosSize = onlyAppNos.size();
                for (int i = 0; i < onlyAppNosSize; i++) {
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), onlyAppNos.get(i).getApply(), null,
                            "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, onlyAppNos.get(i).getCreateTime(), null, null);
                }
            }

            if (null != onlyOrderNos && !onlyOrderNos.isEmpty()) {
                int onlyOrderNosSize = onlyOrderNos.size();
                for (int i = 0; i < onlyOrderNosSize; i++) {
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(), null, onlyOrderNos.get(i).getOrder(),
                            "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, onlyOrderNos.get(i).getCreateTime(), null, null);
                }
            }

            if (null != applyRelateOrderNos && !applyRelateOrderNos.isEmpty()) {
                int applyRelateOrderNosSize = applyRelateOrderNos.size();
                for (int i = 0; i < applyRelateOrderNosSize; i++) {
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "has_device_num", memberDeviceAndApplyAndOrderBean.getHasDeviceNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "has_ip_num", memberDeviceAndApplyAndOrderBean.getHasIpNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "has_merchant_num", memberDeviceAndApplyAndOrderBean.getHasMerchantNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "has_appl_num", memberDeviceAndApplyAndOrderBean.getHasApplNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                    AddIndexDatas(memberIndexDatas, Long.valueOf(memberAndPhoneBean.getMemberId()), memberAndPhoneBean.getPhones(),
                            applyRelateOrderNos.get(i).getApply(), applyRelateOrderNos.get(i).getOrder(),
                            "has_order_num", memberDeviceAndApplyAndOrderBean.getHasOrderNum(), 0, applyRelateOrderNos.get(i).getCreateTime(), null, null);
                }
            }

            updateMemberIndex(memberIndexDatas);

            if (null != sameDeviceBeanList){
                sameDeviceBeanList.clear();
                sameDeviceBeanList = null;
            }

            if (null != sameIpBeanList){
                sameIpBeanList.clear();
                sameIpBeanList = null;
            }
        }


    }


    private static void dealAllBasicDataByApplyList(List<MemberAndPhoneBean> memberAndPhoneBeanList, ODatabaseDocumentTx tx, boolean isAllData) {
        int size = memberAndPhoneBeanList.size();


        int memberRelatedPhoneNoSize = size / ConstantHelper.MEMBER_RELATED_PHONENO_SIZE;
        int memberRelatedPhoneNoRemainder = size % ConstantHelper.MEMBER_RELATED_PHONENO_SIZE;

        for (int i = 0; i < memberRelatedPhoneNoSize; i++) {
            LOGGER.info("dealAllBasicDataByApplyList i is {}", i);

            List<String> s = new ArrayList<String>();
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (int j = i * ConstantHelper.MEMBER_RELATED_PHONENO_SIZE; j < (i + 1) * ConstantHelper.MEMBER_RELATED_PHONENO_SIZE; j++) {
                String phone = memberAndPhoneBeanList.get(j).getPhones();
                map.put(phone, j);
                s.add(phone);
            }

            String sql = "select @rid as phoneRid0,phone as phone, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone in " + s.toString();
            OResultSet phoneInfos = tx.command(new OCommandSQL(sql)).execute(new Object[]{});
            if (phoneInfos == null || phoneInfos.size() <= 0) {
                LOGGER.error("dealAllBasicDataByApplyList phone {} is empty", s.toString());
                return;
            }
            int phoneInfosSize = phoneInfos.size();
            for (int k = 0; k < phoneInfosSize; k++) {
                String phone = ((ODocument) phoneInfos.get(k)).field("phone");
                int index = map.get(phone);
                try {
                    dealBasicDataByPhone(memberAndPhoneBeanList.get(index), tx, (ODocument) phoneInfos.get(k), isAllData);
                } catch (RuntimeException e) {
                    LOGGER.error("dealAllBasicDataByApplyList is error {}", e);
                }
            }

            if (map != null) {
                map.clear();
                map = null;
            }

            if (s != null) {
                s.clear();
                s = null;
            }
        }

        if (memberRelatedPhoneNoRemainder > 0) {
            List<String> s = new ArrayList<String>();
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (int m = memberRelatedPhoneNoSize * ConstantHelper.MEMBER_RELATED_PHONENO_SIZE; m < size; m++) {
                String phone = memberAndPhoneBeanList.get(m).getPhones();
                map.put(phone, m);
                s.add(phone);
            }
            String sql = "select @rid as phoneRid0,phone as phone, unionall(in_CallTo,out_CallTo) as callTos,in('HasPhone') as members0 from Phone where phone in " + s.toString();
            OResultSet phoneInfos = tx.command(new OCommandSQL(sql)).execute(new Object[]{});
            if (phoneInfos == null || phoneInfos.size() <= 0) {
                LOGGER.error("dealAllBasicDataByApplyList phone {} is empty", s.toString());
                return;
            }
            int phoneInfosSize = phoneInfos.size();
            for (int k = 0; k < phoneInfosSize; k++) {
                String phone = ((ODocument) phoneInfos.get(k)).field("phone");
                int index = map.get(phone);
                try {
                    dealBasicDataByPhone(memberAndPhoneBeanList.get(index), tx, (ODocument) phoneInfos.get(k), isAllData);
                } catch (RuntimeException e) {
                    LOGGER.error("dealAllBasicDataByApplyList is error {}", e);
                }
            }

            if (map != null) {
                map.clear();
                map = null;
            }
            if (s != null) {
                s.clear();
                s = null;
            }
        }
    }

    private static void exportToCsv(List<IndexData> indexDatas, List<IndexData> deviceIndexDataList, List<IndexData> ipIndexDataList, List<IndexData> memberIndexDatas) {
        File csvFile = new File(ConstantHelper.FILE_PATH, ConstantHelper.PHONETAG_FILE_NAME);
        List<List<String>> data = new ArrayList<List<String>>();

        int indexDatasSize = indexDatas.size();
        for (int p = 0; p < indexDatasSize; p++) {
            List<String> list = new ArrayList<String>();
            IndexData indexData = indexDatas.get(p);
            list.add(String.valueOf(indexData.getMemberId()));
            if (null != indexData.getApplyNo()) {
                list.add(indexData.getApplyNo() + "\t");
            } else {
                list.add("");
            }
            if (null != indexData.getOrderNo()) {
                list.add(indexData.getOrderNo() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getMobile());
            list.add(indexData.getIndexName());
            list.add(String.valueOf(indexData.getDirect()));
            list.add(String.valueOf(indexData.getIndirect()));
            list.add(indexData.getCreateTime() + "\t");
            if (null != indexData.getUpdateTime()) {
                list.add(indexData.getUpdateTime() + "\t");
            } else {
                list.add("");
            }
            data.add(list);
        }
        CSVUtils.appendDate(csvFile, data);


        //
        File deviceCsvFile = new File(ConstantHelper.FILE_PATH, ConstantHelper.DEIVE_FILE_NAME);
        List<List<String>> deviceData = new ArrayList<List<String>>();

        int deviceIndexDataListSize = deviceIndexDataList.size();
        for (int p = 0; p < deviceIndexDataListSize; p++) {
            List<String> list = new ArrayList<String>();
            IndexData indexData = deviceIndexDataList.get(p);
            list.add(String.valueOf(indexData.getMemberId()));
            if (null != indexData.getApplyNo()) {
                list.add(indexData.getApplyNo() + "\t");
            } else {
                list.add("");
            }
            if (null != indexData.getOrderNo()) {
                list.add(indexData.getOrderNo() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getDeviceId());
            list.add(indexData.getIndexName());
            list.add(String.valueOf(indexData.getDirect()));
            list.add(indexData.getCreateTime() + "\t");
            if (null != indexData.getUpdateTime()) {
                list.add(indexData.getUpdateTime() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getMobile());
            deviceData.add(list);
        }
        CSVUtils.appendDate(deviceCsvFile, deviceData);

        //
        File ipCsvFile = new File(ConstantHelper.FILE_PATH, ConstantHelper.IP_FILE_NAME);
        List<List<String>> ipData = new ArrayList<List<String>>();

        int ipIndexDataListSize = ipIndexDataList.size();
        for (int p = 0; p < ipIndexDataListSize; p++) {
            List<String> list = new ArrayList<String>();
            IndexData indexData = ipIndexDataList.get(p);
            list.add(String.valueOf(indexData.getMemberId()));
            if (null != indexData.getApplyNo()) {
                list.add(indexData.getApplyNo() + "\t");
            } else {
                list.add("");
            }
            if (null != indexData.getOrderNo()) {
                list.add(indexData.getOrderNo() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getIp());
            list.add(indexData.getIndexName());
            list.add(String.valueOf(indexData.getDirect()));
            list.add(indexData.getCreateTime() + "\t");
            if (null != indexData.getUpdateTime()) {
                list.add(indexData.getUpdateTime() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getMobile());
            ipData.add(list);
        }
        CSVUtils.appendDate(ipCsvFile, ipData);

        //
        File memberCsvFile = new File(ConstantHelper.FILE_PATH, ConstantHelper.MEMBER_FILE_NAME);
        List<List<String>> memberData = new ArrayList<List<String>>();

        int memberIndexDatasSize = memberIndexDatas.size();
        for (int p = 0; p < memberIndexDatasSize; p++) {
            List<String> list = new ArrayList<String>();
            IndexData indexData = memberIndexDatas.get(p);
            list.add(String.valueOf(indexData.getMemberId()));
            if (null != indexData.getApplyNo()) {
                list.add(indexData.getApplyNo() + "\t");
            } else {
                list.add("");
            }
            if (null != indexData.getOrderNo()) {
                list.add(indexData.getOrderNo() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getIndexName());
            list.add(String.valueOf(indexData.getDirect()));
            list.add(indexData.getCreateTime() + "\t");
            if (null != indexData.getUpdateTime()) {
                list.add(indexData.getUpdateTime() + "\t");
            } else {
                list.add("");
            }
            list.add(indexData.getMobile());
            memberData.add(list);
        }
        CSVUtils.appendDate(memberCsvFile, memberData);
    }


    private static void insertPhonetagIndex(List<IndexData> indexDatas) {
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != indexDatas) {
            PreparedStatement pstmt = null;
            try {
                pstmt = mysqlConn.prepareStatement("insert into phonetag_index (member_id, apply_no, order_no,mobile,index_name,direct,indirect,create_time) " +
                        "values(?,?,?,?,?,?,?,?)");

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
                LOGGER.error("insertPhonetagIndex has e is {}", e);
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
                pstmt = mysqlConn.prepareStatement("insert into device_index (member_id, apply_no, order_no,mobile,deviceId,index_name,direct,create_time) " +
                        "values(?,?,?,?,?,?,?,?)");

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

                pstmt = mysqlConn.prepareStatement("insert into ip_index (member_id, apply_no, order_no,mobile,ip,index_name,direct,create_time) " +
                        "values(?,?,?,?,?,?,?,?)");

                for (int i = 0; i < ipIndexDatas.size(); i++) {
                    pstmt.setLong(1, ipIndexDatas.get(i).getMemberId());
                    pstmt.setString(2, ipIndexDatas.get(i).getApplyNo());
                    pstmt.setString(3, ipIndexDatas.get(i).getOrderNo());
                    pstmt.setString(4, ipIndexDatas.get(i).getMobile());
                    pstmt.setString(5, ipIndexDatas.get(i).getIp());
                    pstmt.setString(6, ipIndexDatas.get(i).getIndexName());
                    pstmt.setLong(7, ipIndexDatas.get(i).getDirect());
                    pstmt.setString(8, ipIndexDatas.get(i).getCreateTime());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            } catch (Exception e) {
                LOGGER.error("insertDeviceAndIpIndex has e is {}", e);
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

    private static void updateDeviceIndex(List<IndexData> deviceIndexDatas){
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != deviceIndexDatas && deviceIndexDatas.size() > 0) {
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                int num = 0;
                for (int i = 0; i < deviceIndexDatas.size(); i++) {
                    if (null != deviceIndexDatas.get(i).getApplyNo()){
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `device_index` where apply_no = ? and deviceId = ?");
                        pstmt.setString(1,  deviceIndexDatas.get(i).getApplyNo());
                        pstmt.setString(2,  deviceIndexDatas.get(i).getDeviceId());
                    }else{
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `device_index` where order_no = ? and deviceId = ?");
                        pstmt.setString(1,  deviceIndexDatas.get(i).getOrderNo());
                        pstmt.setString(2,  deviceIndexDatas.get(i).getDeviceId());
                    }

                    rs = pstmt.executeQuery();
                    while (rs.next()){
                        num = rs.getInt("num");
                    }
                    if (num > 0){
                        if (null != deviceIndexDatas.get(i).getApplyNo()){
                            pstmt = mysqlConn.prepareStatement("update device_index set  direct = ? ,update_time = now() where apply_no = ? and deviceId = ? ");
                        }else{
                            pstmt = mysqlConn.prepareStatement("update device_index set  direct = ? ,update_time = now() where order_no = ? and deviceId = ?");
                        }

                        pstmt.setLong(1, deviceIndexDatas.get(i).getDirect());
                        if (null != deviceIndexDatas.get(i).getApplyNo()){
                            pstmt.setString(2, deviceIndexDatas.get(i).getApplyNo());
                        }else{
                            pstmt.setString(2, deviceIndexDatas.get(i).getOrderNo());
                        }
                        pstmt.setString(3, deviceIndexDatas.get(i).getDeviceId());
                        pstmt.executeUpdate();
                    }else{
                        pstmt = mysqlConn.prepareStatement("insert into device_index (member_id, apply_no, order_no,mobile,deviceId,index_name,direct,create_time) " +
                                "values(?,?,?,?,?,?,?,?)");
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
                    LOGGER.error("updateDeviceIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("updateDeviceIndex mysqlConn.close have e {}", e);
                }
            }
        }
    }


    private static void updateIpIndex(List<IndexData> deviceIndexDatas){
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != deviceIndexDatas  && deviceIndexDatas.size() > 0) {
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                int num = 0;
                for (int i = 0; i < deviceIndexDatas.size(); i++) {
                    if (null != deviceIndexDatas.get(i).getApplyNo()){
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `ip_index` where apply_no = ? and ip = ?");
                        pstmt.setString(1,  deviceIndexDatas.get(i).getApplyNo());
                        pstmt.setString(2,  deviceIndexDatas.get(i).getIp());
                    }else{
                        pstmt = mysqlConn.prepareStatement("SELECT count(1) as num FROM `ip_index` where order_no = ? and ip = ?");
                        pstmt.setString(1,  deviceIndexDatas.get(i).getOrderNo());
                        pstmt.setString(2,  deviceIndexDatas.get(i).getIp());
                    }

                    rs = pstmt.executeQuery();
                    while (rs.next()){
                        num = rs.getInt("num");
                    }
                    if (num > 0){
                        if (null != deviceIndexDatas.get(i).getApplyNo()){
                            pstmt = mysqlConn.prepareStatement("update ip_index set  direct = ? ,update_time = now() where apply_no = ? and ip = ? ");
                        }else{
                            pstmt = mysqlConn.prepareStatement("update ip_index set  direct = ? ,update_time = now() where order_no = ? and ip = ?");
                        }

                        pstmt.setLong(1, deviceIndexDatas.get(i).getDirect());
                        if (null != deviceIndexDatas.get(i).getApplyNo()){
                            pstmt.setString(2, deviceIndexDatas.get(i).getApplyNo());
                        }else{
                            pstmt.setString(2, deviceIndexDatas.get(i).getOrderNo());
                        }
                        pstmt.setString(3, deviceIndexDatas.get(i).getIp());
                        pstmt.executeUpdate();
                    }else{
                        pstmt = mysqlConn.prepareStatement("insert into ip_index (member_id, apply_no, order_no,mobile,ip,index_name,direct,create_time) " +
                                "values(?,?,?,?,?,?,?,?)");
                        pstmt.setLong(1, deviceIndexDatas.get(i).getMemberId());
                        pstmt.setString(2, deviceIndexDatas.get(i).getApplyNo());
                        pstmt.setString(3, deviceIndexDatas.get(i).getOrderNo());
                        pstmt.setString(4, deviceIndexDatas.get(i).getMobile());
                        pstmt.setString(5, deviceIndexDatas.get(i).getIp());
                        pstmt.setString(6, deviceIndexDatas.get(i).getIndexName());
                        pstmt.setLong(7, deviceIndexDatas.get(i).getDirect());
                        pstmt.setString(8, deviceIndexDatas.get(i).getCreateTime());
                        pstmt.addBatch();
                    }
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
                    LOGGER.error("updateIpIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("updateIpIndex mysqlConn.close have e {}", e);
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
                pstmt = mysqlConn.prepareStatement("insert into member_index (member_id, apply_no, order_no,mobile,index_name,direct,create_time) " +
                        "values(?,?,?,?,?,?,?)");

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
                LOGGER.error("insertMemberIndex has e is {}", e);
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

    private static void updateMemberIndex(List<IndexData> memberIndexDatas) {
        Connection mysqlConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbSourceUrl"),
                ConfigUtils.getProperty("mysqlDbUserName"), ConfigUtils.getProperty("mysqlDbUserPassword"));

        if (null != memberIndexDatas && memberIndexDatas.size() > 0) {
            PreparedStatement pstmt = null;
            try {
                for (int i = 0; i < memberIndexDatas.size(); i++) {
                    if (null != memberIndexDatas.get(i).getApplyNo()){
                        pstmt = mysqlConn.prepareStatement("update member_index set  direct = ? ,update_time = now() where apply_no = ? and index_name = ?");
                    }else{
                        pstmt = mysqlConn.prepareStatement("update member_index set  direct = ? ,update_time = now() where order_no = ? and index_name = ?");
                    }

                    pstmt.setLong(1, memberIndexDatas.get(i).getDirect());
                    if (null != memberIndexDatas.get(i).getApplyNo()){
                        pstmt.setString(2, memberIndexDatas.get(i).getApplyNo());
                    }else{
                        pstmt.setString(2, memberIndexDatas.get(i).getOrderNo());
                    }
                    pstmt.setString(3,memberIndexDatas.get(i).getIndexName());
                    pstmt.executeUpdate();
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("updateMemberIndex pstmt.close have e {}", e);
                }
                try {
                    if (mysqlConn != null) {
                        mysqlConn.close();
                    }
                } catch (Exception e) {
                    LOGGER.error("updateMemberIndex mysqlConn.close have e {}", e);
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

        if (isAllDataQueryFlag) {
            ODatabaseDocumentTx tx = getODataBaseDocumentTx();
            OResultSet memberHasDevice = tx.command(new OCommandSQL("select deviceId as deviceId, in_MemberHasDevice as memberHasDevice from device where in_MemberHasDevice.size() > 0")).execute(new Object[]{});
            int memberHasDeviceSize = memberHasDevice.size();
            for (int i = 0; i < memberHasDeviceSize; i++) {
                ODocument inMemberHasDevice = ((ODocument) memberHasDevice.get(i));
                String deviceId = inMemberHasDevice.field("deviceId");
                ORidBag ocrs = inMemberHasDevice.field("memberHasDevice");

                if (null != ocrs && !ocrs.isEmpty()) {
                    int ocrsSize = ocrs.size();
                    deviceHasMemeberMap.put(deviceId, ocrsSize);
                    Iterator<OIdentifiable> it = ocrs.iterator();
                    while (it.hasNext()) {
                        ODocument ocr = (ODocument) it.next();
                        ODocument member = ocr.field("out");
                        long memberId = member.field("memberId");
                        if (memberHasDeviceMap.containsKey(memberId)) {
                            memberHasDeviceMap.get(memberId).add(deviceId);
                        } else {
                            List<String> list = new ArrayList<String>();
                            list.add(deviceId);
                            memberHasDeviceMap.put(memberId, list);
                        }
                    }
                }
            }

            OResultSet memberHasIp = tx.command(new OCommandSQL("select ip as ip, in_MemberHasIp as memberHasIp from ip where in_MemberHasIp.size() > 0")).execute(new Object[]{});
            int memberHasIpSize = memberHasIp.size();
            for (int i = 0; i < memberHasIpSize; i++) {
                ODocument inMemberHasIp = ((ODocument) memberHasIp.get(i));
                String ip = inMemberHasIp.field("ip");
                ORidBag ocrs = inMemberHasIp.field("memberHasIp");

                if (null != ocrs && !ocrs.isEmpty()) {
                    int ocrsSize = ocrs.size();
                    ipHasMemeberMap.put(ip, ocrsSize);

                    Iterator<OIdentifiable> it = ocrs.iterator();
                    while (it.hasNext()) {
                        ODocument ocr = (ODocument) it.next();
                        ODocument member = ocr.field("out");
                        long memberId = member.field("memberId");
                        if (memberHasIpMap.containsKey(memberId)) {
                            memberHasIpMap.get(memberId).add(ip);
                        } else {
                            List<String> list = new ArrayList<String>();
                            list.add(ip);
                            memberHasIpMap.put(memberId, list);
                        }
                    }
                }
            }
            if (tx != null) {
                OrientDbUtils.close(tx);
            }
        }

        Connection mysqlBusinesConn = DbUtils.getConnection(ConfigUtils.getProperty("mysqlDbBusinessSourceUrl"),
                ConfigUtils.getProperty("mysqlDbBusinessUserName"), ConfigUtils.getProperty("mysqlDbBusinessUserPassword"));

        ExecutorService es = new ThreadPoolExecutor(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainCorePoolSize"))
                , Integer.parseInt(ConfigUtils.getProperty("allDataImportMainMaximumPoolSize")),
                Long.parseLong(ConfigUtils.getProperty("allDataImportMainKeepAliveTime")), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(Integer.parseInt(ConfigUtils.getProperty("allDataImportMainQueueLength"))), new HanlderThreadFactory());

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int memberAndPhoneCount = 0;
        Set<String> memberInfoMapSet = new HashSet<String>();
        Map<String, List<ApplyRelateOrder>> memberInfoOnlyApplyMap = new HashMap<String, List<ApplyRelateOrder>>();
        Map<String, List<ApplyRelateOrder>> memberInfoApplyRelateOrderMap = new HashMap<String, List<ApplyRelateOrder>>();
        Map<String, List<ApplyRelateOrder>> memberInfoOnlyOrderMap = new HashMap<String, List<ApplyRelateOrder>>();

        try {
            //查询总数
            if (!isAllDataQueryFlag) {
                String onlyApplySql = "SELECT apply_no as apply_no,member_id as member_id,cellphone as phone,created_datetime as created_datetime,store_id as store_id FROM apply_info where (DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? and modified_datetime is null) or (DATE_FORMAT(modified_datetime,'%Y-%m-%d') = ?) and order_no is null";
                String applyWithOrderSql = "SELECT apply_no as apply_no,member_id as member_id,cellphone as phone, order_no as order_no,created_datetime as created_datetime,store_id as store_id FROM apply_info where (DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? and modified_datetime is null) or (DATE_FORMAT(modified_datetime,'%Y-%m-%d') = ?) and order_no is not null";
                String onlyOrderSql = "SELECT order_no as order_no, member_id as member_id, mobile as phone,created_datetime as created_datetime,store_id as store_id FROM money_box_order where (DATE_FORMAT(created_datetime,'%Y-%m-%d') = ? and modified_datetime is null) or (DATE_FORMAT(modified_datetime,'%Y-%m-%d') = ?)";
                memberAndPhoneCount = getDataFromMysql(pstmt, rs, mysqlBusinesConn, memberInfoOnlyApplyMap, memberInfoApplyRelateOrderMap, memberInfoOnlyOrderMap,
                        memberInfoMapSet, memberAndPhoneCount, onlyApplySql, applyWithOrderSql, onlyOrderSql,isAllDataQueryFlag,date);
            } else {

                String[] phonetagColNames = {"member_id", "apply_no", "order_no", "mobile", "index_name", "direct", "indirect", "create_time", "update_time"};
                CSVUtils.createFileAndColName(ConstantHelper.FILE_PATH, ConstantHelper.PHONETAG_FILE_NAME, phonetagColNames);

                String[] deviceColNames = {"member_id", "apply_no", "order_no", "deviceId", "index_name", "direct", "create_time", "update_time", "mobile"};
                CSVUtils.createFileAndColName(ConstantHelper.FILE_PATH, ConstantHelper.DEIVE_FILE_NAME, deviceColNames);

                String[] ipColNames = {"member_id", "apply_no", "order_no", "ip", "index_name", "direct", "create_time", "update_time", "mobile"};
                CSVUtils.createFileAndColName(ConstantHelper.FILE_PATH, ConstantHelper.IP_FILE_NAME, ipColNames);

                String[] memberColNames = {"member_id", "apply_no", "order_no", "index_name", "direct", "create_time", "update_time", "mobile"};
                CSVUtils.createFileAndColName(ConstantHelper.FILE_PATH, ConstantHelper.MEMBER_FILE_NAME, memberColNames);

                String onlyApplySql = "SELECT apply_no as apply_no,member_id as member_id,cellphone as phone,created_datetime as created_datetime,store_id as store_id FROM apply_info where order_no is null";
                String applyWithOrderSql = "SELECT apply_no as apply_no,member_id as member_id,cellphone as phone, order_no as order_no,created_datetime as created_datetime,store_id as store_id FROM apply_info where order_no is not null";
                String onlyOrderSql = "SELECT order_no as order_no, member_id as member_id, mobile as phone,created_datetime as created_datetime,store_id as store_id FROM money_box_order";
                memberAndPhoneCount = getDataFromMysql(pstmt, rs, mysqlBusinesConn, memberInfoOnlyApplyMap, memberInfoApplyRelateOrderMap, memberInfoOnlyOrderMap,
                         memberInfoMapSet, memberAndPhoneCount, onlyApplySql, applyWithOrderSql, onlyOrderSql,isAllDataQueryFlag,date);
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
            if (memberAndPhoneBeanArrayList != null) {
                memberAndPhoneBeanArrayList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (memberInfoOnlyApplyMap != null) {
                memberInfoOnlyApplyMap.clear();
            }
            if (memberInfoOnlyOrderMap != null) {
                memberInfoOnlyApplyMap.clear();
            }
            if (memberInfoApplyRelateOrderMap != null) {
                memberInfoOnlyApplyMap.clear();
            }

            if (memberInfoMapSet != null) {
                memberInfoMapSet.clear();
            }
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


    /**
     *
     * @param pstmt
     * @param rs
     * @param mysqlBusinesConn
     * @param memberInfoOnlyApplyMap
     * @param memberInfoApplyRelateOrderMap
     * @param memberInfoOnlyOrderMap
     * @param memberInfoMapSet
     * @param memberAndPhoneCount
     * @param onlyApplySql
     * @param applyWithOrderSql
     * @param onlyOrderSql
     * @return
     * @throws Exception
     */
    private static int getDataFromMysql(PreparedStatement pstmt, ResultSet rs,Connection mysqlBusinesConn,
                                   Map<String, List<ApplyRelateOrder>> memberInfoOnlyApplyMap,Map<String, List<ApplyRelateOrder>> memberInfoApplyRelateOrderMap,
                                   Map<String, List<ApplyRelateOrder>> memberInfoOnlyOrderMap,Set<String> memberInfoMapSet,int memberAndPhoneCount,
                                        String onlyApplySql,String applyWithOrderSql,String onlyOrderSql,
                                        Boolean isAllDataQueryFlag,String date) throws Exception{
        List<String> tempOrderList = new ArrayList<String>();
        //这个sql查询的是只有申请的用户
        pstmt = mysqlBusinesConn.prepareStatement(onlyApplySql);
        if (!isAllDataQueryFlag){
            pstmt.setString(1, date);
            pstmt.setString(2, date);
        }
        rs = pstmt.executeQuery();

        while (rs.next()) {
            String applyNo = rs.getString("apply_no");
            Long memberId = rs.getLong("member_id");
            String phone = rs.getString("phone");
            String createdDatetime = rs.getString("created_datetime");
            String storeId = rs.getString("store_id");
            String memberInfoMapKey = (new StringBuilder(String.valueOf(memberId)).append(",").append(phone)).toString();

            ApplyRelateOrder applyRelateOrder = new ApplyRelateOrder();
            applyRelateOrder.setApply(applyNo);
            applyRelateOrder.setCreateTime(createdDatetime);
            if (memberInfoOnlyApplyMap.containsKey(memberInfoMapKey)) {
                memberInfoOnlyApplyMap.get(memberInfoMapKey).add(applyRelateOrder);
            } else {
                List<ApplyRelateOrder> list = new ArrayList<ApplyRelateOrder>();
                list.add(applyRelateOrder);
                memberInfoOnlyApplyMap.put(memberInfoMapKey, list);
            }

            addStoreIdToMap(storeId);
        }

        //这个sql查询的是有申请关联订单的用户
        pstmt = mysqlBusinesConn.prepareStatement(applyWithOrderSql);
        if (!isAllDataQueryFlag){
            pstmt.setString(1, date);
            pstmt.setString(2, date);
        }
        rs = pstmt.executeQuery();

        while (rs.next()) {
            String applyNo = rs.getString("apply_no");
            String orderNo = rs.getString("order_no");
            String createdDatetime = rs.getString("created_datetime");
            tempOrderList.add(orderNo);

            ApplyRelateOrder applyRelateOrder = new ApplyRelateOrder();
            applyRelateOrder.setApply(applyNo);
            applyRelateOrder.setOrder(orderNo);
            applyRelateOrder.setCreateTime(createdDatetime);
            Long memberId = rs.getLong("member_id");
            String phone = rs.getString("phone");
            String storeId = rs.getString("store_id");
            String memberInfoMapKey = (new StringBuilder(String.valueOf(memberId)).append(",").append(phone)).toString();
            if (memberInfoApplyRelateOrderMap.containsKey(memberInfoMapKey)) {
                memberInfoApplyRelateOrderMap.get(memberInfoMapKey).add(applyRelateOrder);
            } else {
                List<ApplyRelateOrder> list = new ArrayList<ApplyRelateOrder>();
                list.add(applyRelateOrder);
                memberInfoApplyRelateOrderMap.put(memberInfoMapKey, list);
            }

            addStoreIdToMap(storeId);
        }
        //这个sql查询的是有订单去除有申请的用户
        pstmt = mysqlBusinesConn.prepareStatement(onlyOrderSql);
        if (!isAllDataQueryFlag){
            pstmt.setString(1, date);
            pstmt.setString(2, date);
        }
        rs = pstmt.executeQuery();

        while (rs.next()) {
            String orderNo = rs.getString("order_no");
            String createdDatetime = rs.getString("created_datetime");
            String storeId = rs.getString("store_id");

            if (!tempOrderList.contains(orderNo)) {
                Long memberId = rs.getLong("member_id");
                String phone = rs.getString("phone");
                String memberInfoMapKey = (new StringBuilder(String.valueOf(memberId)).append(",").append(phone)).toString();

                ApplyRelateOrder applyRelateOrder = new ApplyRelateOrder();
                applyRelateOrder.setOrder(orderNo);
                applyRelateOrder.setCreateTime(createdDatetime);
                if (memberInfoOnlyOrderMap.containsKey(memberInfoMapKey)) {
                    memberInfoOnlyOrderMap.get(memberInfoMapKey).add(applyRelateOrder);
                } else {
                    List<ApplyRelateOrder> list = new ArrayList<ApplyRelateOrder>();
                    list.add(applyRelateOrder);
                    memberInfoOnlyOrderMap.put(memberInfoMapKey, list);
                }
            }

            addStoreIdToMap(storeId);
        }
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

        return memberAndPhoneCount;
    }


    private static void addStoreIdToMap(String storeId) {
        if (null != storeId) {
            if (memberHasStoreMap.containsKey(memberId)) {
                memberHasStoreMap.get(memberId).add(storeId);
            } else {
                Set<String> set = new HashSet<String>();
                set.add(storeId);
                memberHasStoreMap.put(memberId, set);
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
                pstmt = mysqlConn.prepareStatement("delete FROM phonetag_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ? and update_time is null");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM ip_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ? and update_time is null");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM device_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ? and update_time is null");
                pstmt.setString(1, date);
                pstmt.executeUpdate();

                pstmt = mysqlConn.prepareStatement("delete FROM member_index where DATE_FORMAT(create_time,'%Y-%m-%d') = ? and update_time is null");
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
