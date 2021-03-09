package com.ego.mapreduce.common;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;

public class LinkedMapWritableComparable implements WritableComparable<LinkedMapWritableComparable> {

    private LinkedHashMap<String, Object> instance;

    /** default constructor. */
    public LinkedMapWritableComparable() {
        super();
        this.instance = new LinkedHashMap<>();
    }

    public void put(String key, Object value) {
        instance.put(key, value);
    }

    @Override
    public String toString() {
        return this.instance.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = this.instance.size();
        for (String key:this.instance.keySet()){
            if (this.instance.get(key) instanceof Integer){

            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // todo
    }

    @Override
    public int compareTo(LinkedMapWritableComparable o) {
        return 0;
    }
}
