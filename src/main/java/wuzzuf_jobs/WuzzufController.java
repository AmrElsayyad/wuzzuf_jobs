package wuzzuf_jobs;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

@SuppressWarnings("SpellCheckingInspection")
@RestController
@RequestMapping(path = "")
public class WuzzufController {

    private final WuzzufDaoImpl wuzzufDao;

    @Autowired
    public WuzzufController(WuzzufDaoImpl wuzzufDao) {
        this.wuzzufDao = wuzzufDao;
        this.wuzzufDao.readDataset("src/main/resources/Wuzzuf_Jobs.csv");
    }

    public static ResponseEntity<String> getResponse(String body) {
        return ResponseEntity.ok(
                "<!DOCTYPE html>\n" +
                        "<html lang=\"en\" xmlns:font-color=\"http://www.w3.org/1999/xhtml\">\n" +
                        "<head>\n" +
                        "    <meta charset=\"UTF-8\" name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n" +
                        "    <title>Wuzzuf Jobs Analysis</title>\n" +
                        "    <style>\n" +
                        "         * {box-sizing: border-box}\n" +
                        "         body {\n" +
                        "         font-family: \"Lato\", cursive;\n" +
                        "         background-color: skyblue;\n" +
                        "         }\n" +
                        "         /* Style the tab */\n" +
                        "         .tab {\n" +
                        "         float: left;\n" +
                        "         border: 0px solid #ccc;\n" +
                        "         background-color: #f1f1f1;\n" +
                        "         width: none;\n" +
                        "         height: none;\n" +
                        "         }\n" +
                        "         /* Style the buttons inside the tab */\n" +
                        "         .tab button {\n" +
                        "         display: block;\n" +
                        "         background-color: inherit;\n" +
                        "         color: black;\n" +
                        "         padding: 15px 20px;\n" +
                        "         width: 100%;\n" +
                        "         border: 1px solid #ccc;\n" +
                        "         outline: none;\n" +
                        "         text-align: left;\n" +
                        "         cursor: pointer;\n" +
                        "         transition: 0.3s;\n" +
                        "         font-size: 17px;\n" +
                        "         }\n" +
                        "         /* Change background color of buttons on hover */\n" +
                        "         .tab button:hover {\n" +
                        "         background-color: #ddd;\n" +
                        "         }\n" +
                        "         /* Create an active/current \"tab button\" class */\n" +
                        "         .tab button.active {\n" +
                        "         background-color: #ccc;\n" +
                        "         }\n" +
                        "         /* Style the tab content */\n" +
                        "         .tabcontent {\n" +
                        "         float: left;\n" +
                        "         padding: 0px 25px;\n" +
                        "         width: 78%;\n" +
                        "         }\n" +
                        "         /* Style the link */\n" +
                        "         .link {\n" +
                        "         text-decoration: none;\n" +
                        "         color: black;\n" +
                        "         }\n" +
                        "         table {\n" +
                        "         font-family: arial, sans-serif;\n" +
                        "         border-collapse: collapse;\n" +
                        "         }\n" +
                        "         td, th {\n" +
                        "         border: 1px solid;\n" +
                        "         text-align: left;\n" +
                        "         padding: 8px;\n" +
                        "         }\n" +
                        "         tr:nth-child(even) {\n" +
                        "         background-color: #dddddd;\n" +
                        "         }\n" +
                        "      </style>\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "<h2 style=\"padding: 0px 10px\">Wuzzuf Jobs Analysis</h2>\n" +
                        "<div class=\"tab\">\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/View-Dataset'\">View Dataset</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Dataset-Structure'\">Dataset Structure</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Dataset-Summary'\">Dataset Summary</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Clean-Dataset'\">Clean Dataset</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Demanding-Companies'\">Most Demanding Companies</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Demanding-Companies-Pie-Chart'\">Most Demanding Companies Pie Chart</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Popular-Job-Titles'\">Most Popular Job Titles</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Popular-Job-Titles-Bar-Chart'\">Most Popular Job Titles Bar Chart</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Popular-Areas'\">Most Popular Areas</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Popular-Areas-Bar-Chart'\">Most Popular Areas Bar Chart</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Most-Demanded-Skills'\">Most Demanded Skills</button>\n" +
                        "    <button class=\"tablinks\" onclick=\"location.href='http://localhost:8080/Factorize-YearsExp'\">Factorize Years of Experience</button>\n" +
                        "</div>\n" +
                        "<div class=\"tabcontent\">\n" +
                        body + "\n" +
                        "</div>\n" +
                        "</body>\n" +
                        "</html>"
        );
    }

    public static String datasetToTable(Dataset<?> dataset) {
        String header =
                "<table>\n" +
                "  <tr>\n" +
                "    <th>" +
                        String.join(
                                "</th>\n" +
                                "    <th>",
                                dataset.columns()) +
                "</th>\n" +
                        "  </tr>\n";
        String body = dataset.toDF().collectAsList()
                .stream()
                .map(row ->
                        "  <tr>\n" +
                                "    <td>" +
                                row.mkString(
                                        "</td>\n" +
                                        "    <td>"))
                .collect(Collectors.joining(
                        "</td>\n" +
                                "  </tr>\n"));
        return header + body;
    }

    @GetMapping("/")
    public String home() throws IOException {
        return FileUtils.readFileToString(new File("src/main/resources/templates/home.html"));
    }

    @GetMapping(path = "View-Dataset")
    public ResponseEntity<String> ViewDataset() {
        return getResponse(datasetToTable(wuzzufDao.getDataset()));
    }

    @GetMapping(path = "Dataset-Structure")
    public ResponseEntity<String> DatasetStructure() {
        return getResponse(wuzzufDao.getStructure().treeString().replaceAll("\\|", "</br>\\|"));
    }

    @GetMapping(path = "Dataset-Summary")
    public ResponseEntity<String> DatasetSummary() {
        return getResponse(datasetToTable(wuzzufDao.getSummary()));
    }

    @GetMapping(path = "Clean-Dataset")
    public ResponseEntity<String> CleanDataset() {
        return getResponse(datasetToTable(wuzzufDao.cleanDataset()));
    }

    @GetMapping(path = "Most-Demanding-Companies")
    public ResponseEntity<String> MostDemandingCompanies() {
        return getResponse(datasetToTable(wuzzufDao.jobsPerCompany()));
    }

    @GetMapping(path = "Most-Demanding-Companies-Pie-Chart")
    public ResponseEntity<String> MostDemandingCompaniesPieChart() throws IOException {
        wuzzufDao.displayPieChart(wuzzufDao.jobsPerCompany(), "Jobs Per Company");
        return getResponse("<img src='JobsPerCompanyPieChart.jpg'>");
    }

    @GetMapping(path = "Most-Popular-Job-Titles")
    public ResponseEntity<String> MostPopularJobTitles() {
        return getResponse(datasetToTable(wuzzufDao.mostPopularJobTitles()));
    }

    @GetMapping(path = "Most-Popular-Job-Titles-Bar-Chart")
    public ResponseEntity<String> MostPopularJobTitlesBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularJobTitles(), "Most Popular Job Titles", "Job Title", "Count");
        return getResponse("<img src='MostPopularJobTitlesBarChart.jpg'>");
    }

    @GetMapping(path = "Most-Popular-Areas")
    public ResponseEntity<String> MostPopularAreas() {
        return getResponse(datasetToTable(wuzzufDao.mostPopularAreas()));
    }

    @GetMapping(path = "Most-Popular-Areas-Bar-Chart")
    public ResponseEntity<String> MostPopularAreasBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularAreas(), "Most Popular Areas", "Area", "Count");
        return getResponse("<img src='MostPopularAreasBarChart.jpg'>");
    }

    @GetMapping(path = "Most-Demanded-Skills")
    public ResponseEntity<String> MostDemandedSkills() {
        return getResponse(datasetToTable(wuzzufDao.getMostDemandedSkills()));
    }

    @GetMapping(path = "Factorize-YearsExp")
    public ResponseEntity<String> FactorizeYearsExp() {
        return getResponse(datasetToTable(wuzzufDao.factorizeColumn("yearsExp")));
    }
}
