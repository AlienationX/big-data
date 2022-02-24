package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Description(
        name = "to_json",
        value = "_FUNC_(string, int, ...) - 将输入字段转换成json格式的字符串",
        extended = "Example:\n"
                + " > SELECT _FUNC_('col1', col1, 'col2', col2) FROM src;"
                + " > SELECT _FUNC_('id', 1, 'name', 'aaa') FROM src;"
)
public class GenericUDFToJson extends GenericUDF {

    private List<String> typeNames = new ArrayList<>();

    // 主要作用是将java的类型转换成writable类型
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length % 2 != 0) {
            throw new UDFArgumentLengthException("the function requires even number, got " + arguments.length);
        }

        // converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            String typeName = arguments[i].getTypeName();
            typeNames.add(typeName);
            // typeName.matches("int|long|float|double")
            // if (typeName.matches("int")) {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            // } else if (typeName.startsWith("long")) {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            // } else if (typeName.startsWith("float")) {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
            // } else if (typeName.startsWith("double")) {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            // } else if (typeName.startsWith("decimal")) {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
            // } else {
            //     converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            // }
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        // return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Map<String, Object> data = new LinkedHashMap<>();
        String key = null;
        for (int i = 0; i < arguments.length; i++) {
            if (i % 2 == 0) {
                key =  arguments[i].get().toString();
            } else {
                String typeName = typeNames.get(i);
                // System.out.println(typeName);  // 具体的值就是 int, string, double 等

                // 放到hive中执行返回的json存在问题，必须转换value的类型，不能使用Object
                // select to_json('name','zhangsan','age',18,'score',88.8);
                // {"name":{"bytes":[122,104,97,110,103,115,97,110],"length":8}}  # gson
                // {"name":{"bytes":"emhhbmdzYW4=","length":8}}  # fastjson
                String value = arguments[i].get().toString();
                if (typeName.matches("int")) {
                    data.put(key, Integer.parseInt(value));
                } else if (typeName.matches("float")) {
                    data.put(key, Float.parseFloat(value));
                } else if (typeName.matches("double")) {
                    data.put(key, Double.parseDouble(value));
                } else if (typeName.matches("decimal")) {
                    data.put(key, Double.parseDouble(value));
                } else {
                    data.put(key, arguments[i].get().toString());
                }
            }
        }

        Gson gson = new Gson();
        return new Text(gson.toJson(data));
    }

    @Override
    public String getDisplayString(String[] children) {
        // 这个方法不知道有什么用啊！！！
        // return "Usage: now()";
        return getStandardDisplayString("to_json", children);
    }

    public static void main(String[] args) throws Exception {
        GenericUDFToJson udf = new GenericUDFToJson();
        udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        });
        Text result = (Text) udf.evaluate(new DeferredObject[]{
                new DeferredJavaObject("name"),
                new DeferredJavaObject("香茗"),
                new DeferredJavaObject("age"),
                new DeferredJavaObject(21),
                new DeferredJavaObject("score"),
                new DeferredJavaObject(88.8)
        });
        System.out.println(result);
    }
}
