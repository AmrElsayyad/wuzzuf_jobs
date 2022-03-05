package wuzzuf_jobs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.knowm.xchart.*;

import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.regexp_replace;

public class wuzzufDaoImpl implements wuzzufDao {

    private Dataset<job> dataset;

    @Override
    public Dataset<job> getDataset() {
        return dataset;
    }

    @Override
    public void setDataset(Dataset<job> dataset) {
        this.dataset = dataset;
    }

    @Override
    public Dataset<job> mapDataset(Dataset<Row> dataset) {
        return dataset.map(wuzzufDao::rowToJob, Encoders.bean(job.class));
    }

    @Override
    public Dataset<job> readDataset(String filename) {

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("WuzzufJobs");
        builder.master("local[*]");

        final SparkSession sparkSession = builder.getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");

        dataset = mapDataset(dataFrameReader.csv(filename));

        return dataset;
    }

    @Override
    public Dataset<job> cleanDataset(boolean inline) {

        Dataset<job> ds = dataset;

        // remove " Yrs of Exp" from "YearsExp" column
        System.out.println("Removing \" Yrs of Exp\" from \"YearsExp\" column...\n");
        ds = mapDataset(ds.withColumn("yearsExp",
                regexp_replace(ds.col("yearsExp"), " Yrs of Exp", "")));
        ds.show();
        System.out.println();

        // count null values in "YearsExp" column
        ds.createOrReplaceTempView("wuzzuf");
        ds.sqlContext().sql(
                "SELECT COUNT(*) AS YearsExp_nulls " +
                        "FROM wuzzuf " +
                        "WHERE YearsExp == \"null\""
        ).show();

        // drop jobs with "YearsExp" equal null
        System.out.println("Dropping jobs with \"YearsExp\" = null...\n");
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
    public Dataset<Row> getMostDemandedSkills() {

        dataset.createOrReplaceTempView("wuzzuf");
        Dataset<Row> skills_ds = dataset.sqlContext().sql("SELECT skills FROM wuzzuf");

        skills_ds.createOrReplaceTempView("SkillsView");
        return skills_ds.sqlContext().sql(
                "SELECT skill, count(skill) AS count " +
                        "FROM ( " +
                        "   SELECT EXPLODE(SPLIT(skills, ',')) AS skill " +
                        "   FROM SkillsView) " +
                        "GROUP BY skill " +
                        "ORDER BY count DESC "
        );
    }

    @Override
    public void displayPieChart(Dataset<Row> dataset, String title) {

        PieChart pieChart = new PieChartBuilder().title(title).build();
        for (int i = 0; i < 5; i++) {
            Row job = dataset.collectAsList().get(i);
            pieChart.addSeries(job.getString(0), job.getLong(1));
        }
        pieChart.addSeries("Other", dataset.except(dataset.limit(5)).count());
        new SwingWrapper<>(pieChart).displayChart();

    }

    @Override
    public void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel) {

        CategoryChart barChart = new CategoryChartBuilder().title(title).xAxisTitle(xLabel).yAxisTitle(yLabel).build();
        barChart.getStyler().setXAxisLabelRotation(45);
        barChart.addSeries(xLabel,
                dataset.limit(10).collectAsList().stream().map(job -> job.getString(0)).collect(Collectors.toList()),
                dataset.limit(10).collectAsList().stream().map(job -> job.getLong(1)).collect(Collectors.toList())
        );
        //noinspection rawtypes,unchecked
        new SwingWrapper(barChart).displayChart();

    }

}
