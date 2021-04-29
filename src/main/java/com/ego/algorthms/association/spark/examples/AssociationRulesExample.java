package com.ego.algorthms.association.spark.examples;

import com.ego.HadoopUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class AssociationRulesExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Association Rules")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

        // FreqItemset是频繁项集的结果集，基本构成是组合项目+频次，类似JavaPairRDD<list<String>, Long>
        // 不推荐，还是基于RDD的操作
        JavaRDD<FreqItemset<String>> freqItemsets = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(Arrays.asList(
                new FreqItemset<>(new String[]{"a"}, 15L),
                new FreqItemset<>(new String[]{"b"}, 35L),
                new FreqItemset<>(new String[]{"a", "b"}, 12L)
        ));
        freqItemsets.collect().forEach(System.out::println);

        AssociationRules rules = new AssociationRules()
                .setMinConfidence(0.8);
        JavaRDD<Rule<String>> results = rules.run(freqItemsets);

        for (Rule<String> rule : results.collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence()
            );
        }
    }
}
