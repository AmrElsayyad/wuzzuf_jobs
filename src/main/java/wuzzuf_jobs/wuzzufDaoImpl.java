package wuzzuf_jobs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;

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
    public Dataset<Row> cleanDataset() {

        Dataset<Row> ds = dataset;

        // remove " Yrs of Exp" from "YearsExp" column
        System.out.println("Removing \" Yrs of Exp\" from \"YearsExp\" column");
        ds = ds.withColumn("YearsExp", regexp_replace(ds.col("YearsExp"), " Yrs of Exp", ""));
        ds.show();

        // count null values in "YearsExp" column
        ds.createOrReplaceTempView("wuzzuf");
        ds.sqlContext().sql(
                "SELECT COUNT(*) AS YearsExp_nulls " +
                        "FROM wuzzuf " +
                        "WHERE YearsExp == \"null\""
        ).show();

        // drop rows with "YearsExp" equal null
        System.out.println("Dropping rows with \"YearsExp\" equal null");
        ds = ds.where("YearsExp <> \"null\"");
        ds.summary().show();

        // remove duplicates
        System.out.println("Removing duplicates");
        ds = ds.dropDuplicates();
        ds.summary().show();

        return ds;
    }

    @Override
    public void jobsPerCompany() {

        dataset.createOrReplaceTempView("wuzzuf");
        Dataset<Row> jobsPerCompany = dataset.sqlContext().sql(
                "SELECT Company, COUNT(*) AS jobs_count " +
                        "FROM wuzzuf " +
                        "GROUP BY Company " +
                        "ORDER BY jobs_count DESC"
        );

        PieChart pieChart = new PieChartBuilder().title("Jobs per company").build();
        for (int i = 0; i < 5; i++) {
            Row row = jobsPerCompany.collectAsList().get(i);
            pieChart.addSeries(row.getString(0), row.getLong(1));
        }
        pieChart.addSeries("Other", jobsPerCompany.except(jobsPerCompany.limit(5)).count());
        new SwingWrapper<>(pieChart).displayChart();

    }
}
