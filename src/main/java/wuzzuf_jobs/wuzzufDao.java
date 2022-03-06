package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public interface wuzzufDao {

    Dataset<job> getDataset();

    void setDataset(Dataset<job> dataset);

    static job rowToJob(Row row) {
        StructType schema = row.schema();
        int title = -1, company = -1, location = -1, type = -1, level = -1, yearsExp = -1, country = -1, skills = -1;
        String[] fieldNames = schema.fieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase("title")) {
                title = i;
            } else if (fieldNames[i].equalsIgnoreCase("company")) {
                company = i;
            } else if (fieldNames[i].equalsIgnoreCase("location")) {
                location = i;
            } else if (fieldNames[i].equalsIgnoreCase("type")) {
                type = i;
            } else if (fieldNames[i].equalsIgnoreCase("level")) {
                level = i;
            } else if (fieldNames[i].equalsIgnoreCase("yearsexp")) {
                yearsExp = i;
            } else if (fieldNames[i].equalsIgnoreCase("country")) {
                country = i;
            } else if (fieldNames[i].equalsIgnoreCase("skills")) {
                skills = i;
            }
        }
        return new job(row.getString(title), row.getString(company), row.getString(location), row.getString(type), row.getString(level), row.getString(yearsExp), row.getString(country), row.getString(skills));
    }

    Dataset<job> mapDataset(Dataset<Row> dataset);

    Dataset<job> readDataset(String filename);

    Dataset<job> cleanDataset(boolean inline);

    Dataset<Row> jobsPerCompany();

    Dataset<Row> mostPopularJobTitles();

    Dataset<Row> mostPopularAreas();

    Dataset<Row> getMostDemandedSkills();

    Dataset<Row> factorizeColumn(String column);

    void displayPieChart(Dataset<Row> dataset, String title);

    void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel);

}
