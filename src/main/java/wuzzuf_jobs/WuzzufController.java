package wuzzuf_jobs;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

@SuppressWarnings("SpellCheckingInspection")
@Controller
public class WuzzufController {

    private final WuzzufDaoImpl wuzzufDao;

    @Autowired
    public WuzzufController(WuzzufDaoImpl wuzzufDao) {
        this.wuzzufDao = wuzzufDao;
        this.wuzzufDao.readDataset("src/main/resources/Wuzzuf_Jobs.csv");
    }

    public static ResponseEntity<String> getResponse(String body) throws IOException {
        return ResponseEntity.ok(
                FileUtils.readFileToString(new File("src/main/resources/templates/template.html"))
                        .replaceFirst("<!-- here -->",
                                "<div class='w3-table-all'>" + body + "</div>")
        );
    }

    public static String datasetToTable(Dataset<?> dataset) {
        String header = "<table><tr><th>" + String.join("</th><th>", dataset.columns()) + "</th></tr>";
        String body = dataset.toDF().collectAsList().stream().map(row -> "<tr><td>" + row.mkString("</td><td>"))
                .collect(Collectors.joining("</td></tr>"));
        return header + body;
    }

    @GetMapping("/")
    public String home() {
        return "home";
    }

    @GetMapping("View_Dataset")
    public ResponseEntity<String> ViewDataset() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getDataset()));
    }

    @GetMapping("Dataset_Structure")
    public ResponseEntity<String> DatasetStructure() throws IOException {
        return getResponse(wuzzufDao.getStructure().treeString().replaceAll("root\n \\|-- ", "")
                .replaceAll(" \\|-- ", "</br>"));
    }

    @GetMapping("Dataset_Summary")
    public ResponseEntity<String> DatasetSummary() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getSummary()));
    }

    @GetMapping("Clean_Dataset")
    public ResponseEntity<String> CleanDataset() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.cleanDataset()));
    }

    @GetMapping("Most_Demanding_Companies")
    public ResponseEntity<String> MostDemandingCompanies() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.jobsPerCompany()));
    }

    @GetMapping("Most_Demanding_Companies_Pie_Chart")
    public ResponseEntity<String> MostDemandingCompaniesPieChart() throws IOException {
        wuzzufDao.displayPieChart(wuzzufDao.jobsPerCompany(), "Jobs Per Company");
        return getResponse("<img src='JobsPerCompanyPieChart.jpg'>");
    }

    @GetMapping("Most_Popular_Job_Titles")
    public ResponseEntity<String> MostPopularJobTitles() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.mostPopularJobTitles()));
    }

    @GetMapping("Most_Popular_Job_Titles_Bar_Chart")
    public ResponseEntity<String> MostPopularJobTitlesBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularJobTitles(), "Most Popular Job Titles", "Job Title", "Count");
        return getResponse("<img src='MostPopularJobTitlesBarChart.jpg'>");
    }

    @GetMapping("Most_Popular_Areas")
    public ResponseEntity<String> MostPopularAreas() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.mostPopularAreas()));
    }

    @GetMapping("Most_Popular_Areas_Bar_Chart")
    public ResponseEntity<String> MostPopularAreasBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularAreas(), "Most Popular Areas", "Area", "Count");
        return getResponse("<img src='MostPopularAreasBarChart.jpg'>");
    }

    @GetMapping("Most_Demanded_Skills")
    public ResponseEntity<String> MostDemandedSkills() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getMostDemandedSkills()));
    }

    @GetMapping("Factorize_YearsExp")
    public ResponseEntity<String> FactorizeYearsExp() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.factorizeColumn("yearsExp")));
    }
}
