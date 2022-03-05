package wuzzuf_jobs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;

import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.regexp_replace;

public class wuzzufDaoImpl implements wuzzufDao {

    private Dataset<Row> dataset;

    @Override
    public Dataset<Row> getDataset() {
        return dataset;
    }

    @Override
    public void setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    @Override
    public Dataset<Row> readDataset(String filename) {

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("WuzzufJobs");
        builder.master("local[*]");

        final SparkSession sparkSession = builder.getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");


        dataset = dataFrameReader.csv(filename);

        return dataset;
    }

    @Override
    public Dataset<Row> cleanDataset(boolean inline) {

        Dataset<Row> ds = dataset;

        // remove " Yrs of Exp" from "YearsExp" column
        System.out.println("Removing \" Yrs of Exp\" from \"YearsExp\" column...\n");
        ds = ds.withColumn("YearsExp", regexp_replace(ds.col("YearsExp"), " Yrs of Exp", ""));
        ds.show();
        System.out.println();

        // count null values in "YearsExp" column
        ds.createOrReplaceTempView("wuzzuf");
        ds.sqlContext().sql(
                "SELECT COUNT(*) AS YearsExp_nulls " +
                        "FROM wuzzuf " +
                        "WHERE YearsExp == \"null\""
        ).show();

        // drop rows with "YearsExp" equal null
        System.out.println("Dropping rows with \"YearsExp\" = null...\n");
        ds = ds.where("YearsExp <> \"null\"");
        ds.summary().show();
        System.out.println();

        // remove duplicates
        System.out.println("Removing duplicates...\n");
        ds = ds.dropDuplicates();
        ds.summary().show();
        System.out.println();

        // check if inline
        if (inline) {
            dataset = ds;
        }

        return ds;
    }

    @Override
    public Dataset<Row> jobsPerCompany() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Company, COUNT(*) AS jobs_count " +
                        "FROM wuzzuf " +
                        "GROUP BY Company " +
                        "ORDER BY jobs_count DESC"
        );
    }

    @Override
    public Dataset<Row> mostPopularJobTitles() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Title, COUNT(*) AS Count " +
                        "FROM wuzzuf " +
                        "GROUP BY Title " +
                        "ORDER BY Count DESC"
        );
    }

    @Override
    public Dataset<Row> mostPopularAreas() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Location, COUNT(*) AS Count " +
                        "FROM wuzzuf " +
                        "GROUP BY Location " +
                        "ORDER BY Count DESC"
        );
    }

    @Override
    public void displayPieChart(Dataset<Row> dataset, String title) {

        PieChart pieChart = new PieChartBuilder().title(title).build();
        for (int i = 0; i < 5; i++) {
            Row row = dataset.collectAsList().get(i);
            pieChart.addSeries(row.getString(0), row.getLong(1));
        }
        pieChart.addSeries("Other", dataset.except(dataset.limit(5)).count());
        new SwingWrapper<>(pieChart).displayChart();

    }

    @Override
    public void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel) {

        CategoryChart barChart = new CategoryChartBuilder().title(title).xAxisTitle(xLabel).yAxisTitle(yLabel).build();
        barChart.getStyler().setXAxisLabelRotation(45);
        barChart.addSeries(xLabel,
                dataset.limit(10).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList()),
                dataset.limit(10).collectAsList().stream().map(row -> row.getLong(1)).collect(Collectors.toList())
        );
        new SwingWrapper(barChart).displayChart();

    }

}
