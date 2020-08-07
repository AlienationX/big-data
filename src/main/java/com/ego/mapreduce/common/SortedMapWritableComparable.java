package com.ego.mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class SortedMapWritableComparable implements WritableComparable<SortedMapWritableComparable> {

    private SortedMap<WritableComparable, Writable> instance;

    /** default constructor. */
    public SortedMapWritableComparable() {
        super();
        this.instance = new TreeMap<WritableComparable, Writable>();
    }

    public Writable put(WritableComparable key, Writable value) {
        return instance.put(key, value);
    }

    @Override
    public String toString() {
        return this.instance.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // todo
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // todo
    }

    @Override
    public int compareTo(SortedMapWritableComparable o) {
        return 0;
    }
}
