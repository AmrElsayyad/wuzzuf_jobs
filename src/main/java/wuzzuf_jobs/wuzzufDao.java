package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface wuzzufDao {

    Dataset<Row> getDataset();

    void setDataset(Dataset<Row> dataset);

    Dataset<Row> readDataset(String filename);

    Dataset<Row> cleanDataset();

}
