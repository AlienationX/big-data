package com.ego.examples;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

public class CaseJson {

    public static void complexJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map jsonMap = mapper.readValue(jsonString, LinkedHashMap.class);
        System.out.println(jsonMap.get("data"));
        System.out.println(jsonMap.get("data").getClass());
        System.out.println(jsonMap.get("data"));
        List<List<String>> data = new ArrayList<>();
        for (Map<String, Object> map : (List<Map<String, Object>>) jsonMap.get("data")) {
            List<String> rowList = new ArrayList();
            Map<String, Object> rowMap = (Map<String, Object>) map.get("values");
            for (Object val : rowMap.values()) {
                System.out.println(val);
                if (val == null) {
                    val = "null";
                }
                rowList.add(val.toString());
            }
            data.add(rowList);
        }
        System.out.println(data);
    }

    public static void parseJson(String jsonString){
        JSONObject jsonObject = JSONObject.parseObject(jsonString);

        // 格式化输出JSON
        String pretty = JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
        System.out.println(String.format("原始JSON：\r\n%s", pretty));

        // 获取JSON第一层所有的key
        Set<String> keys = jsonObject.keySet();
        // 获取第一层每个key对应的值 的类型
        for (String key : keys) {
            System.out.println(String.format("%s(key)：%s(值类型)", key, jsonObject.get(key).getClass().getSimpleName()));
        }
    }

    public static void main(String[] args) throws IOException {
        String jsonString = "{\"log_file\": \"mysql-bin.000155\", \"log_pos\": 13422, \"log_time\": \"2021-05-18 15:24:17\", \"log_timestamp\": 1621322657, \"schema\": \"test\", \"table\": \"test4\", \"table_pk\": \"id\", \"action\": \"insert\", \"data\": [{\"values\": {\"id\": 25, \"data\": \"kafak-7\", \"data2\": null}}]}";
        System.out.println(jsonString);

        ObjectMapper mapper = new ObjectMapper();

        // json字符串转为Map对象
        Map jsonMap = mapper.readValue(jsonString, Map.class);
        System.out.println(jsonMap);

        // 对象转json字符串
        jsonString = mapper.writeValueAsString(jsonMap);
        System.out.println(jsonString);

        complexJson(jsonString);
        parseJson(jsonString);
    }

}
