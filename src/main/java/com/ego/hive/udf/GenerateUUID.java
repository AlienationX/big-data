package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.UUID;

/**
 * @Describtion注解是可选的 用于对函数进行说明，其中的FUNC字符串表示函数名，当使用DESCRIBE FUNCTION命令时，替换成函数名。
 * @Describtion包含三个属性：
 *
 * name：用于指定Hive中的函数名。
 * value：用于描述函数的参数。必须使用_FUNC_会自动识别替换。
 * extended：额外的说明，如，给出示例。当使用DESCRIBE FUNCTION EXTENDED name的时候打印。
 *
 * describe function uuid;
 * describe function extended uuid;
 */

@Description(
        name = "uuid",
        value = "_FUNC_() - No parameters required, "
                + "returns the value that is uuid.",
        extended = "Example:\n"
                + " > SELECT _FUNC_() FROM src;"
)
public class GenerateUUID extends UDF {

    public String evaluate() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public static void main(String[] args) {
        GenerateUUID gu = new GenerateUUID();
        for (int i = 0; i < 10; i++) {
            System.out.println(gu.evaluate());
        }
    }
}
