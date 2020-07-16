package com.ego.mr.sql;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * create table tmp.mr_group stored as parquet as
 * select 'roger' as name,'2020-01-01' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'milk' as item,5 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'bread' as item,10 as amount union all
 * select 'roger' as name,'2020-01-02' as dt,'orange' as item,3 as amount union all
 * select 'mike' as name,'2020-01-01' as dt,'bread' as item,12.5 as amount union all
 * select 'mike' as name,'2020-01-02' as dt,'orange' as item,6 as amount union all
 * select 'mike' as name,'2020-01-03' as dt,'orange' as item,6 as amount;
 * <p>
 * select name,
 * count(distinct item) as item_num,
 * sum(amount) as sum_amount,
 * avg(amount) as avg_amount,
 * max(amount) as min_amount
 * from tmp.mr_group
 * group by name;
 */

public class GroupCountMinMax {
    private static Logger LOG = Logger.getLogger(GroupCountMinMax.class);


    public static class GroupMap extends Mapper<WritableComparable, HCatRecord, Object, HCatRecord> {
        private HCatSchema schema;

        @Override
        public void setup(Context context) throws IOException {
            schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(WritableComparable key, HCatRecord value, Context context) throws InterruptedException, IOException {
            String name = value.get("name",schema).toString();
            // double amount = (double) value.get("amount",schema);
            HCatRecord record = new DefaultHCatRecord();
            record.setString("item",schema,value.get("item",schema).toString());
            record.set("amount",schema,value.get(Integer.parseInt("amount")));
            context.write(new Text(name), record);
        }

    }

    public static class GroupReduce extends Reducer<Text, HCatRecord, WritableComparable, HCatRecord> {
        private HCatSchema schema;

        @Override
        public void setup(Context context) throws IOException {
            schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void reduce(Text key, Iterable<HCatRecord> values, Context context) throws IOException, InterruptedException {


            Iterator<HCatRecord> iter = values.iterator();
            while (iter.hasNext()){

            }
            HCatRecord record = new DefaultHCatRecord();
            record.set("name", schema,);
            context.write(NullWritable.get(), record);

        }
    }

    public static void main(String[] args) {

    }
}
