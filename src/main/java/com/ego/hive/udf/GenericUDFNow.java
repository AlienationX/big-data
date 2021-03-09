package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Description(
        name = "now",
        value = "_FUNC_() - no parameters required, returns current timestamp",
        extended = "Example:\n"
                + " > SELECT _FUNC_() FROM src;"
)

public class GenericUDFNow extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //这个方法只调用一次，并且在evaluate()方法之前调用。该方法接受的参数是一个ObjectInspectors数组。该方法检查接受正确的参数类型和参数个数。
        if (arguments.length != 0) {
            throw new UDFArgumentException("the function no parameters required");
        }
        // 设置返回类型
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaDateObjectInspector
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
        // 返回字符类型：PrimitiveObjectInspectorFactory.javaStringObjectInspector
        // 返回是否类型：PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        // javaBooleanObjectInspector 和 writableBooleanObjectInspector 都可以
        return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。
        return Timestamp.valueOf(LocalDateTime.now());
    }

    @Override
    public String getDisplayString(String[] children) {
        //这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
        return "Usage: now()";
    }
}
