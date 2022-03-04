package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class jobsAnalysis {

    public static void main(String[] args) {

        wuzzufDaoImpl dao = new wuzzufDaoImpl();
        Dataset<Row> ds = dao.readDataset("src/main/resources/Wuzzuf_Jobs.csv");

        // display summary statistics
        System.out.println("Summary:");
        ds.printSchema();
        ds.summary().show();

        // displaying the dataset
        System.out.println("Dataset:");
        ds.show();

        // cleaning dataset
        ds = dao.cleanDataset();
        ds.show();

    }

}
