package wuzzuf_jobs;

import org.apache.spark.sql.*;

public interface wuzzufDao {

    Dataset<Row> getDataset();

    void setDataset(Dataset<Row> dataset);

    Dataset<Row> readDataset(String filename);

    Dataset<Row> cleanDataset(boolean inline);

    Dataset<Row> jobsPerCompany();

    Dataset<Row> mostPopularJobTitles();

    Dataset<Row> mostPopularAreas();

    void displayPieChart(Dataset<Row> dataset, String title);

    void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel);

}
