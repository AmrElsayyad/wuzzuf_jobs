package wuzzuf_jobs;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class wuzzufDoaImpl implements wuzzufDoa {

    public Dataset<Row> readDataSet(String filename) {

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("wuzzuf CSV Analysis Demo");
        builder.master("local[*]");

        final SparkSession sparkSession = builder.getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");

        return dataFrameReader.csv(filename);
    }

    public void displayDataSet(Dataset<Row> csvDataFrame) {

        csvDataFrame.printSchema();
        csvDataFrame.show(10);

    }
}
