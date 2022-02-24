package com.ego.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * select explode_str(name, ',') from dual
 * explode传入参数为array或map，该explode_str函数传入字符串并指定split分隔的字符，就是多提供了个split功能
 */

@Description(name = "explode_str", value = "_FUNC_(a, b) - separates the elements of array a into multiple rows," + " or the elements of a map into multiple rows and columns ")
public class GenericUDTFExplodeStr extends GenericUDTF {

    private transient ObjectInspector[] inputOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("explode_str() takes two string arguments");
        }

        // System.out.println("getTypeName=" + args[0].getTypeName());
        // System.out.println("getCategory=" + args[0].getCategory());
        // if (!args[0].getTypeName().equals("string") && !args[1].getTypeName().equals("string")) {
        //     throw new UDFArgumentException("explode_str() takes two string parameters");
        // }
        if (((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
                && ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "ExplodeStringUDTF two string parameters.");
        }


        // 接收输入格式（inspectors），如果要转换成String类型可以不存储，转换成其他数据类型需要将下面传入的Object类型转换成存储的相应数据类型
        inputOI = args;

        // 定义输出格式（inspectors） -- 类似表的定义，需要定义字段名称和字段类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("col");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // switch (args[0].getCategory()) {
        //     case LIST:
        //         inputOI = args[0];
        //         fieldNames.add("col");
        //         fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
        //         break;
        //     case MAP:
        //         inputOI = args[0];
        //         fieldNames.add("key");
        //         fieldNames.add("value");
        //         fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
        //         fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
        //         break;
        //     default:
        //         throw new UDFArgumentException("explode() takes an array or a map as a parameter");
        // }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        // 忽略null值
        if (args[0] == null || args[1] == null) {
            return;
        }

        // 处理一条输入记录，输出若干条结果记录
        String str = ((PrimitiveObjectInspector) inputOI[0]).getPrimitiveJavaObject(args[0]).toString();
        String sep = ((PrimitiveObjectInspector) inputOI[1]).getPrimitiveJavaObject(args[1]).toString();

        String[] records = str.split(sep);
        // forward的参数必须是Object[], 解析一行数据拆分成多个数组
        // 生成两列数据参考 new Object[] { tokens[0], tokens[1] }
        for (String record : records) {
            forward(new Object[]{record});
        }


        // ArrayList<Object[]> result = new ArrayList<Object[]>();
        //
        // // 忽略null值与空值
        // if (name == null || name.isEmpty()) {
        //     return result;
        // }
        //
        // String[] tokens = name.split("\\s+");
        //
        // if (tokens.length == 2){
        //     result.add(new Object[] { tokens[0], tokens[1] });
        // }else if (tokens.length == 4 && tokens[1].equals("and")){
        //     result.add(new Object[] { tokens[0], tokens[3] });
        //     result.add(new Object[] { tokens[2], tokens[3] });
        // }
        // for (String r : records) {
        //     forward(r);  // 收集数据，必须
        // }
    }

    @Override
    public void close() throws HiveException {
        // 当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
    }
}
