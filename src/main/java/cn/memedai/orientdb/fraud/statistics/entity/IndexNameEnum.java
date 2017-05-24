package cn.memedai.orientdb.fraud.statistics.entity;

/**
 * Created by hangyu on 2017/5/2.
 */
public enum IndexNameEnum {

    contact_microfinance_num("contact_microfinance_num", "小贷机构"),
    contact_loan_intermediary_num("contact_loan_intermediary_num", "贷款中介"),
    contact_collect_num("contact_collect_num", "催收电话"),
    contact_fraud_num("contact_fraud_num", "套现中介"),
    contact_channel_intermediary_num("contact_channel_intermediary_num", "渠道中介"),
    contact_competition_num("contact_competition_num", "分期竞品"),
    contact_defraud_num("contact_defraud_num", "诈骗电话"),
    contact_bank_num("contact_bank_num", "银行"),
    contact_high_risk_num("contact_high_risk_num", "高风险号码"),
    contact_merchant_person_num("contact_merchant_person_num", "商户联系人"),
    contact_merchant_num("contact_merchant_num", "合作商户固话"),
    contact_un_merchant_num("contact_un_merchant_num", "非合作医美商户"),
    contact_food_num("contact_food_num", "餐饮"),
    contact_express_num("contact_express_num", "快递"),
    contact_credit_card_num("contact_credit_card_num", "信用卡"),
    contact_merchant_legal_num("contact_merchant_legal_num", "商户法人"),

    contact_microfinance_120s_num("contact_microfinance_120s_num", "小贷机构120"),
    contact_loan_intermediary_120s_num("contact_loan_intermediary_120s_num", "贷款中介120"),
    contact_collect_120s_num("contact_collect_120s_num", "催收电话120"),
    contact_fraud_120s_num("contact_fraud_120s_num", "套现中介120"),
    contact_channel_intermediary_120s_num("contact_channel_intermediary_120s_num", "渠道中介120"),
    contact_competition_120s_num("contact_competition_120s_num", "分期竞品120"),
    contact_defraud_120s_num("contact_defraud_120s_num", "诈骗电话120"),
    contact_bank_120s_num("contact_bank_120s_num", "银行120"),
    contact_high_risk_120s_num("contact_high_risk_120s_num", "高风险号码120"),
    contact_merchant_person_120s_num("contact_merchant_person_120s_num", "商户联系人120"),
    contact_merchant_120s_num("contact_merchant_120s_num", "合作商户固话120"),
    contact_un_merchant_120s_num("contact_un_merchant_120s_num", "非合作医美商户120"),
    contact_food_120s_num("contact_food_120s_num", "餐饮120"),
    contact_express_120s_num("contact_express_120s_num", "快递120"),
    contact_credit_card_120s_num("contact_credit_card_120s_num", "信用卡120"),
    contact_merchant_legal_120s_num("contact_merchant_legal_120s_num", "商户法人120");

    private String value;

    private String chineseValue;

    IndexNameEnum(String value, String chineseValue) {
        this.value = value;
        this.chineseValue = chineseValue;
    }

    public static String fromValue(String value) {
        if (contact_microfinance_num.chineseValue.equals(value)) {
            return contact_microfinance_num.getValue();
        } else if (contact_loan_intermediary_num.chineseValue.equals(value)) {
            return contact_loan_intermediary_num.getValue();
        } else if (contact_collect_num.chineseValue.equals(value)) {
            return contact_collect_num.getValue();
        } else if (contact_fraud_num.chineseValue.equals(value)) {
            return contact_fraud_num.getValue();
        } else if (contact_channel_intermediary_num.chineseValue.equals(value)) {
            return contact_channel_intermediary_num.getValue();
        } else if (contact_competition_num.chineseValue.equals(value)) {
            return contact_competition_num.getValue();
        } else if (contact_defraud_num.chineseValue.equals(value)) {
            return contact_defraud_num.getValue();
        } else if (contact_bank_num.chineseValue.equals(value)) {
            return contact_bank_num.getValue();
        } else if (contact_high_risk_num.chineseValue.equals(value)) {
            return contact_high_risk_num.getValue();
        } else if (contact_merchant_person_num.chineseValue.equals(value)) {
            return contact_merchant_person_num.getValue();
        } else if (contact_merchant_num.chineseValue.equals(value)) {
            return contact_merchant_num.getValue();
        } else if (contact_un_merchant_num.chineseValue.equals(value)) {
            return contact_un_merchant_num.getValue();
        } else if (contact_food_num.chineseValue.equals(value)) {
            return contact_food_num.getValue();
        } else if (contact_express_num.chineseValue.equals(value)) {
            return contact_express_num.getValue();
        } else if (contact_credit_card_num.chineseValue.equals(value)) {
            return contact_credit_card_num.getValue();
        } else if (contact_merchant_legal_num.chineseValue.equals(value)) {
            return contact_merchant_legal_num.getValue();
        }
        else if (contact_microfinance_120s_num.chineseValue.equals(value)) {
            return contact_microfinance_120s_num.getValue();
        } else if (contact_loan_intermediary_120s_num.chineseValue.equals(value)) {
            return contact_loan_intermediary_120s_num.getValue();
        } else if (contact_collect_120s_num.chineseValue.equals(value)) {
            return contact_collect_120s_num.getValue();
        } else if (contact_fraud_120s_num.chineseValue.equals(value)) {
            return contact_fraud_120s_num.getValue();
        } else if (contact_channel_intermediary_120s_num.chineseValue.equals(value)) {
            return contact_channel_intermediary_120s_num.getValue();
        } else if (contact_competition_120s_num.chineseValue.equals(value)) {
            return contact_competition_120s_num.getValue();
        } else if (contact_defraud_120s_num.chineseValue.equals(value)) {
            return contact_defraud_120s_num.getValue();
        } else if (contact_bank_120s_num.chineseValue.equals(value)) {
            return contact_bank_120s_num.getValue();
        } else if (contact_high_risk_120s_num.chineseValue.equals(value)) {
            return contact_high_risk_120s_num.getValue();
        } else if (contact_merchant_person_120s_num.chineseValue.equals(value)) {
            return contact_merchant_person_120s_num.getValue();
        } else if (contact_merchant_120s_num.chineseValue.equals(value)) {
            return contact_merchant_120s_num.getValue();
        } else if (contact_un_merchant_120s_num.chineseValue.equals(value)) {
            return contact_un_merchant_120s_num.getValue();
        } else if (contact_food_120s_num.chineseValue.equals(value)) {
            return contact_food_120s_num.getValue();
        } else if (contact_express_120s_num.chineseValue.equals(value)) {
            return contact_express_120s_num.getValue();
        } else if (contact_credit_card_120s_num.chineseValue.equals(value)) {
            return contact_credit_card_120s_num.getValue();
        } else if (contact_merchant_legal_120s_num.chineseValue.equals(value)) {
            return contact_merchant_legal_120s_num.getValue();
        }
        else {
            return null;
        }
    }


    public String getValue() {
        return value;
    }
}
