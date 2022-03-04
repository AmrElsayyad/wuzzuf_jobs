package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface wuzzufDoa {
    Dataset<Row> readDataSet(String filename);

    void displayDataSet(Dataset<Row> csvDataFrame);

}
