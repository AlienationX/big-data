package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

@Description(
        name = "all_in_str",
        value = "_FUNC_(string, string) - 查找字符串在当前字符串中出现的所有索引，返回array<int>",
        extended = "Example:\n"
                 + " > SELECT _FUNC_('abcabcd','a') FROM src;"
                 + " > SELECT _FUNC_('abcabcd','ab') FROM src;"
)

public class GenericUDFAllInStr extends GenericUDF {

    // 主要作用是将java的类型转换成writable类型
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //这个方法只调用一次，并且在evaluate()方法之前调用。该方法接受的参数是一个ObjectInspectors数组。该方法检查接受正确的参数类型和参数个数。
        if (arguments.length != 2) {
            throw new UDFArgumentException("the function requires 2 argument");
        }

        // 将java的类型转换成writable类型，否则会报 Caused by: java.lang.ClassCastException: java.lang.Integer cannot be cast to org.apache.hadoop.io.IntWritable
        // System.out.println(arguments[0].getClass());  // org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        // 设置返回类型
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaDateObjectInspector
        // 返回日期类型：PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
        // 返回字符类型：PrimitiveObjectInspectorFactory.javaStringObjectInspector
        // 返回是否类型：PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        // javaBooleanObjectInspector 和 writableBooleanObjectInspector 都可以

        // 强烈推荐上下文都使用hadoop的writable类，包括下面的evaluate等方法。支持返回集合类型 Struct, Map, List, Union
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);  // PrimitiveObjectInspectorFactory.javaIntObjectInspector, evaluate方法的返回必须和该处保持一致
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //这个方法类似UDF的evaluate()方法。它处理真实的数据，并返回最终结果。
        if (arguments[0].get() == null) {
            return null;
        }

        List<IntWritable> result = new ArrayList<>();
        Text word = (Text) converters[0].convert(arguments[0].get());
        Text find = (Text) converters[1].convert(arguments[1].get());
        int wordLength = word.toString().length();
        int findLength = find.toString().length();
        int i = 0;
        while (i < wordLength) {
            int index = word.toString().substring(i).indexOf(find.toString());
            if (index != -1) {
                int value = i + index;
                result.add(new IntWritable(value));
                i = value + findLength;
            } else {
                break;
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        // 这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
        // return "Usage: now()";
        return getStandardDisplayString("all_in_str", children);
    }

    public static void main(String[] args) throws Exception {
        GenericUDFAllInStr udf = new GenericUDFAllInStr();
        udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        });
        List<Integer> result = (List) udf.evaluate(new DeferredObject[]{new DeferredJavaObject("abcabcd"), new DeferredJavaObject("abcd")});
        System.out.println(result);
    }
}
