package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class jobsAnalysis {

    public static void main(String[] args) {

        wuzzufDoaImpl wuzzufDS = new wuzzufDoaImpl();
        // load the data frame using sparkSession
        Dataset<Row> csvDataFrame = wuzzufDS.readDataSet("src/main/resources/Wuzzuf_Jobs.csv");
        wuzzufDS.displayDataSet(csvDataFrame);

    }

}
