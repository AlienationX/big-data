package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Description(
        name = "count_distinct_array",
        value = "_FUNC_(array) - 计算去重后的数组的元素个数，返回int",
        extended = "Example:\n"
                 + " > SELECT _FUNC_(['ab','a','a']) FROM src;"
                 + " > SELECT _FUNC_(array('a','b','b'));"
                 + " > select count_distinct_array(t.items) from (\n" + "select split('a,ab,ab,c,ab',',') as items union all\n" + "select split('a,a,a',',') as items union all\n" + "select split('a,b,c',',') as items\n" + ") t;"
)

public class GenericUDFCountDistinctArray extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //这个方法只调用一次，并且在evaluate()方法之前调用。该方法接受的参数是一个ObjectInspectors数组。该方法检查接受正确的参数类型和参数个数。
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("the function requires 1 argument, got " + arguments.length);
        }

        // System.out.println(arguments[0].getClass());  // org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector
        // System.out.println(arguments[0].getCategory());  // LIST
        // System.out.println(arguments[0].getTypeName());  // 返回值：array<string> 或 array<int> 等
        if (!arguments[0].getTypeName().startsWith("array")) {
            throw new UDFArgumentException("the argument must be array");
        }

        // 设置返回类型
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaDateObjectInspector
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
        // 返回字符类型：PrimitiveObjectInspectorFactory.javaStringObjectInspector
        // 返回是否类型：PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        // javaBooleanObjectInspector 和 writableBooleanObjectInspector 都可以
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。
        List<Object> list = (List<Object>) arguments[0].get();
        return new HashSet<>(list).size();
    }

    @Override
    public String getDisplayString(String[] children) {
        //这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
        // return "Usage: now()";
        return getStandardDisplayString("count_distinct_array", children);
    }

    public static void main(String[] args) throws Exception {
        GenericUDFCountDistinctArray udf = new GenericUDFCountDistinctArray();
        List<String> list = new ArrayList<>();
        list.add("abc");
        list.add("ab");
        list.add("ab");
        list.add("ab");
        udf.initialize(new ObjectInspector[]{ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector)});
        int result = (int) udf.evaluate(new DeferredObject[]{new DeferredJavaObject(list)});
        System.out.println(result);
    }
}
