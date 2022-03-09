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
        return ResponseEntity.ok(FileUtils.readFileToString(new File("src/main/resources/templates/home.html"))
                .replaceFirst("</body>", "<div class=\"tabcontent\">" + body + "</div>" + "</body>"));
    }

    public static String datasetToTable(Dataset<?> dataset) {
        String header = "<table>\n" + "  <tr>\n" + "    <th>" + String.join("</th>\n" + "    <th>", dataset.columns()) + "</th>\n" + "  </tr>\n";
        String body = dataset.toDF().collectAsList().stream().map(row -> "<tr>" + "<td>" + row.mkString("</td>" + "<td>")).collect(Collectors.joining("</td>" + "</tr>"));
        return header + body;
    }

    @GetMapping("/")
    public String home() {
        return "home";
    }

    @GetMapping("View-Dataset")
    public ResponseEntity<String> ViewDataset() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getDataset()));
    }

    @GetMapping("Dataset-Structure")
    public ResponseEntity<String> DatasetStructure() throws IOException {
        return getResponse(wuzzufDao.getStructure().treeString().replaceAll("\\|", "</br>\\|"));
    }

    @GetMapping("Dataset-Summary")
    public ResponseEntity<String> DatasetSummary() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getSummary()));
    }

    @GetMapping("Clean-Dataset")
    public ResponseEntity<String> CleanDataset() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.cleanDataset()));
    }

    @GetMapping("Most-Demanding-Companies")
    public ResponseEntity<String> MostDemandingCompanies() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.jobsPerCompany()));
    }

    @GetMapping("Most-Demanding-Companies-Pie-Chart")
    public ResponseEntity<String> MostDemandingCompaniesPieChart() throws IOException {
        wuzzufDao.displayPieChart(wuzzufDao.jobsPerCompany(), "Jobs Per Company");
        return getResponse("<img src='JobsPerCompanyPieChart.jpg'>");
    }

    @GetMapping("Most-Popular-Job-Titles")
    public ResponseEntity<String> MostPopularJobTitles() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.mostPopularJobTitles()));
    }

    @GetMapping("Most-Popular-Job-Titles-Bar-Chart")
    public ResponseEntity<String> MostPopularJobTitlesBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularJobTitles(), "Most Popular Job Titles", "Job Title", "Count");
        return getResponse("<img src='MostPopularJobTitlesBarChart.jpg'>");
    }

    @GetMapping("Most-Popular-Areas")
    public ResponseEntity<String> MostPopularAreas() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.mostPopularAreas()));
    }

    @GetMapping("Most-Popular-Areas-Bar-Chart")
    public ResponseEntity<String> MostPopularAreasBarChart() throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularAreas(), "Most Popular Areas", "Area", "Count");
        return getResponse("<img src='MostPopularAreasBarChart.jpg'>");
    }

    @GetMapping("Most-Demanded-Skills")
    public ResponseEntity<String> MostDemandedSkills() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.getMostDemandedSkills()));
    }

    @GetMapping("Factorize-YearsExp")
    public ResponseEntity<String> FactorizeYearsExp() throws IOException {
        return getResponse(datasetToTable(wuzzufDao.factorizeColumn("yearsExp")));
    }
}
