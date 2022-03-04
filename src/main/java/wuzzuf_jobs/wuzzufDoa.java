package wuzzuf_jobs;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface wuzzufDoa {
	public Dataset<Row> readDataSet(String filenam);

	public void displayDataSet(Dataset<Row> csvDataFrame);

}
