package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class jobsAnalysis {

	public static void main(String[] args) {

		wuzzufDoaImpl w = new wuzzufDoaImpl();
		// load the data frame using sparkSesson
		Dataset<Row> csvDataFrame = w.readDataSet("src/main/resources/Wuzzuf_Jobs.csv");
		w.displayDataSet(csvDataFrame);

	}

}
