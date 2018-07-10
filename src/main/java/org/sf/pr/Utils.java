package org.sf.pr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.collection.JavaConverters;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    public static JavaSparkContext buildSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("parquet Reader");
        conf.setMaster("local[2]");
        conf.set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("WARN");
        return sparkContext;
    }

    public static SQLContext buildSQLContext() {
        return new SQLContext(buildSparkContext());
    }

    public static DataFrame readParquet(List<String> files) {
        SQLContext sqlContext = buildSQLContext();
        DataFrame df = sqlContext.read().parquet(JavaConverters.asScalaIteratorConverter(files.iterator()).asScala().toSeq());

        df.show();

        return df;
    }

    public static List<Map<String, Object>> showDFData(DataFrame df) {
        Map<String, Object> returnMap = null;
        List<Map<String,Object>> returnlist= new ArrayList<>();
        String [] cols = df.columns();
        for (Row row : df.toJavaRDD().cache().collect())
        {
            returnMap = new HashMap<>();
            for (int i = 0 ; i<row.length() ; i++)
            {
                returnMap.put(cols[i],row.get(i));
            }
            returnlist.add(returnMap);
        }
        return returnlist;
    }
}
