package wuzzuf_jobs;

import org.apache.spark.sql.*;


public class jobsAnalysis {

    public static void main(String[] args) {

        wuzzufDaoImpl dao = new wuzzufDaoImpl();
        Dataset<job> ds = dao.readDataset("src/main/resources/Wuzzuf_Jobs.csv");

        // display summary statistics
        System.out.println("\n=== Summary:\n");
        ds.printSchema();
        ds.summary().show();
        System.out.println();

        // displaying the dataset
        System.out.println("=== Dataset:\n");
        ds.show();
        System.out.println();

        // cleaning dataset
        ds = dao.cleanDataset(true);

        // display dataset after cleaning
        System.out.println("=== Dataset after cleaning:");
        ds.show();
        System.out.println();

        // get number of jobs per company
        System.out.println("=== Jobs per company:\n");
        Dataset<Row> jobsPerCompany = dao.jobsPerCompany();
        jobsPerCompany.show();
        System.out.println();

        // display pie chart for jobs per company
        dao.displayPieChart(jobsPerCompany, "Jobs per company");

        // get most popular job titles
        System.out.println("=== Most Popular Job Titles:\n");
        Dataset<Row> mostPopularJobTitles = dao.mostPopularJobTitles();
        mostPopularJobTitles.show();
        System.out.println();

        // display bar chart of most popular job titles
        dao.displayBarChart(mostPopularJobTitles, "Most Popular Job Titles", "Job Titles", "Job Count");

        // get most popular job areas
        System.out.println("=== Most Popular Areas:\n");
        Dataset<Row> mostPopularAreas = dao.mostPopularAreas();
        mostPopularAreas.show();
        System.out.println();

        // display bar chart of most popular areas
        dao.displayBarChart(mostPopularAreas, "Most Popular Areas", "Area", "Job Count");

        // get most demanded skills
        Dataset<Row> mostDemandedSkills = dao.getMostDemandedSkills();
        mostDemandedSkills.show();

    }

}
