package com.ego.algorthms.association.spark;

import com.ego.HadoopUtil;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MLFPGrowth {

    public static void main(String[] args) {
        //最小支持度
        double minSupport = Double.parseDouble(args[0]);
        //最小置信度
        double minConfidence = Double.parseDouble(args[1]);

        int numPartitions = Integer.parseInt(System.getProperty("param.repartition.num", "0"));

        SparkSession spark = HadoopUtil.createSparkSession("FP-Growth");

        // 注意：transactions的items列表内容必须去重，否则报错org.apache.spark.SparkException: Items in a transaction must be unique but got WrappedArray
        // create table transactions_zy_unique stored as parquet as
        // select t.visitdate, concat_ws(',',collect_set(s.ids)) as clientids
        // from transactions_zy t
        // lateral view explode(split(t.clientids,',')) s as ids
        // group by t.visitdate;
        Dataset<Row> itemsDF = spark.sql("select split(t.clientids,',') as items from tmp.transactions_zy_unique t");

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(minSupport)
                .setMinConfidence(minConfidence)
                .setNumPartitions(numPartitions)
                .fit(itemsDF);

        if (numPartitions != 0) {
            model = new FPGrowth()
                    .setItemsCol("items")
                    .setMinSupport(minSupport)
                    .setMinConfidence(minConfidence)
                    .setNumPartitions(numPartitions)
                    .fit(itemsDF);
        }

        // Display frequent itemsets.
        // model.freqItemsets().show();

        // Display generated association rules.
        // model.associationRules().show();

        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        // model.transform(itemsDF).show();

        Dataset<Row> result = model.freqItemsets();
        result.createOrReplaceTempView("tmp_fp_growth");
        Dataset<Row> df = spark.sql("select t.*, size(t.items) as k from tmp_fp_growth t where size(t.items)>=2");
        df.printSchema();
        df.show(100);

    }
}
