package com.ego.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

// desc function check_id_card; 会返回value部分的值，但是中文存在乱码问题
// desc function extended check_id_card; 会返回value和extended的部分的值
@Description(
        name = "check_id_card",
        value = "_FUNC_(str) - returns int,"
                + " 0: 身份证号合规"
                + "-1: 身份证位数存在问题"
                + "-2: 身份证生日存在问题"
                + "-3: 身份证前两位省份存在问题"
                + "-4: 身份证最后一位校验码存在问题",
        extended = "Example:\n"
                + " > SELECT _FUNC_('1101081999') FROM src;"
                + "   -1"
                + " > SELECT _FUNC_('11010819990231011234') FROM src;"
                + "   -2"
)

public class GenericUDFCheckIDCard extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("the function requires 1 argument");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        int result = 0;
        String s = arguments[0].get().toString();

        // 1.位数校验
        String birthday;
        if (s.length() == 15) {
            birthday = "19" + s.substring(6, 12);
        } else if (s.length() == 18) {
            birthday = s.substring(6, 14);
        } else {
            return -1;
        }

        // 2.生日校验
        if (!isDate(birthday)) {
            return -2;
        }

        // 3.省份校验
        String[] areaCodes = {"11", "12", "13", "14", "15", "21", "22", "23", "31", "32", "33", "34", "35", "36", "37", "41", "42", "43", "44", "45", "46", "50", "51", "52", "53", "54", "61", "62", "63", "64", "65", "71", "81", "82", "91"};
        String areaStr = s.substring(0, 2);
        if (!Arrays.asList(areaCodes).contains(areaStr)) {
            return -3;
        }

        // 4.校验码校验
        String verifyCodes = "10X98765432";
        if (s.length() == 18) {
            if (!StringUtils.isNumeric(s.substring(0, 17))) {
                return -4;
            }
            int valSum =
                    (Integer.parseInt(s.substring(0, 1))
                            + Integer.parseInt(s.substring(10, 11))
                    ) * 7
                            + (Integer.parseInt(s.substring(1, 2))
                            + Integer.parseInt(s.substring(11, 12))
                    ) * 9
                            + (Integer.parseInt(s.substring(2, 3))
                            + Integer.parseInt(s.substring(12, 13))
                    ) * 10
                            + (Integer.parseInt(s.substring(3, 4))
                            + Integer.parseInt(s.substring(13, 14))
                    ) * 5
                            + (Integer.parseInt(s.substring(4, 5))
                            + Integer.parseInt(s.substring(14, 15))
                    ) * 8
                            + (Integer.parseInt(s.substring(5, 6))
                            + Integer.parseInt(s.substring(15, 16))
                    ) * 4
                            + (Integer.parseInt(s.substring(6, 7))
                            + Integer.parseInt(s.substring(16, 17))
                    ) * 2
                            + Integer.parseInt(s.substring(7, 8)
                    )
                            + Integer.parseInt(s.substring(8, 9)
                    ) * 6
                            + Integer.parseInt(s.substring(9, 10)
                    ) * 3;
            int valMod = valSum % 11;
            String checkBit = verifyCodes.substring(valMod, valMod + 1);
            if (!checkBit.equals(s.substring(17, 18))) {
                return -4;
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("check_id_card", children);
    }

    private static boolean isDate(String dateStr) {
        try {
            LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) throws HiveException {
        GenericUDFCheckIDCard udf = new GenericUDFCheckIDCard();
        int result = (int) udf.evaluate(new DeferredObject[]{new DeferredJavaObject("123456")});
        System.out.println(result);
    }

}
