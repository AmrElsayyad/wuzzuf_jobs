package wuzzuf_jobs;

import java.util.List;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class wuzzufDoaImpl implements wuzzufDoa {

	public Dataset<Row> readDataSet(String filenam) {
		// Get DataFrameReader using SparkSession
		// Set header option to true to specify that first row in file contains // name
		// of columns
		final SparkSession sparkSession = SparkSession.builder().appName("wuzzuf CSV Analysis Demo").master("local[2]")
				.getOrCreate();

		final DataFrameReader dataFrameReader = sparkSession.read();

		dataFrameReader.option("header", "true");
		final Dataset<Row> csvDataFrame = dataFrameReader.csv(filenam);
		return csvDataFrame;

	}

	public void displayDataSet(Dataset<Row> csvDataFrame) {
		csvDataFrame.printSchema();
		csvDataFrame.show(10);

			}

}
