package bg.government.opendatapusher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import bg.government.opendatapusher.PushConfig.ConfigRoot;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Hello world!
 *
 */
public class Pusher implements Runnable {
    private static final Logger logger = Logger.getLogger("Pusher");
    
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    
    private static final String UPDATE_OPERATION = "/api/3/action/resource_update";

    private static final String DEFAULT_API_KEY = "xxxxxx";
    
    private String path;
    
    private String apiKey;
    private String rootUrl;
    
    private RestTemplate restTemplate = new RestTemplate();
    
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    
    public static void main(String[] args) throws Exception {
        String startingPath = Pusher.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        startingPath = startingPath.replace("pusher.exe", "").replace("opendata-ckan-pusher.jar", ""); //cleanup for starting as process
        if (!startingPath.endsWith("/") && !startingPath.endsWith("\\")) {
            startingPath = startingPath + "/";
        }
        String path = args.length > 0 ? args[0] : startingPath + "pusher.yml";
        if (!new File(path).exists()) {
            throw new FileNotFoundException(path);
        }
        logger.info("Using config path: " + path);
        executor.scheduleAtFixedRate(new Pusher(path), 0, 5, TimeUnit.MINUTES);
    }
    
    public static void windowsService(String args[]) throws Exception {
        String cmd = "start";
        if (args.length > 0) {
            cmd = args[0];
        }

        if ("start".equals(cmd)) {
            Pusher.main(new String[]{});
        } else {
            executor.shutdownNow();
            System.exit(0);
        }
    }

    public static ConfigRoot parseConfig(String path) throws IOException, JsonParseException,
            JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ConfigRoot config = mapper.readValue(new File(path), ConfigRoot.class);
        return config;
    }

    public Pusher(String path) {
        this.path = path;
    }

    public void run() {
        try {
            // re-parsing config regularly as it may change 
            ConfigRoot configs = parseConfig(path);
            apiKey = configs.getApiKey();
            rootUrl = configs.getRootUrl();
            if (apiKey.equals(DEFAULT_API_KEY)) {
                logger.info("Skipping run, configuration is not supplied");
                return; // not yet configured
            }
            logger.info("Number of configurations to run: " + configs.getConfigs().size());
            for (PushConfig config : configs.getConfigs()) {
                logger.info("Running pusher for " + config.getPath());
                try {
                    File file = getOrCreateFile(config.getTitle());
                    long lastRunMillis = Long.parseLong(Files.readFirstLine(file, Charsets.UTF_8));
                    LocalDateTime lastRun = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastRunMillis), ZoneId.systemDefault());
                    LocalDateTime lastModified = LocalDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()), ZoneId.systemDefault());
                    if (lastRun.isBefore(LocalDateTime.now().minusDays(config.getDays())) || lastRun.isBefore(lastModified)) {
                        String resultPath = null;
                        switch (config.getSourceType()) {
                        case XLS:
                            resultPath = xlsToCsv(config.getPath());
                            break;
                        case SQL:
                            resultPath = sqlToCsv(config.getConnectionString(), config.getQuery(), config.getPath());
                            break;
                        case RAW:
                            resultPath = config.getPath();
                            break;
                        }
                        logger.info("Parsing complete; pushing to opendata portal");
                        
                        pushDataset(resultPath, config.getResourceKey());
                        storeSuccessfulRun(file);
                    } else {
                        logger.info("Skipping run; last run was too recently: " + lastRun + ". File last modified: " + lastModified);
                    }
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, "Problem with resource " + config.getResourceKey(), ex);
                }
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Failed to parse config", ex);
        }
    }

    private void storeSuccessfulRun(File file) throws IOException {
        Files.write(String.valueOf(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()),
                file, Charsets.UTF_8);
    }

    private void pushDataset(String csvPath, String resourceKey) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        
        MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
        map.add("id", resourceKey);
        map.add("upload", new FileSystemResource(csvPath));
        
        HttpEntity<?> request = new  HttpEntity<>(map, headers);
        
        try {
            String url = rootUrl + UPDATE_OPERATION;
            String result = restTemplate.exchange(new URI(url), HttpMethod.POST, request, String.class).getBody();
            logger.info("Result: " + result);
        } catch (HttpServerErrorException e) {
            logger.log(Level.SEVERE, e.getResponseBodyAsString(), e);
        } catch (RestClientException e) {
            throw new IOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        
    }

    private File getOrCreateFile(String title) throws IOException {
        File file = new File(title.replace(" ", "_")  + ".last");
        if (!file.exists()) {
            file.createNewFile();
            // arbitrary start before the current date
            LocalDateTime start = LocalDateTime.of(2000, 1, 1, 0, 0);
            Files.write(String.valueOf(start.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()),
                    file, Charsets.UTF_8);
        }
        return file;
    }

    public String sqlToCsv(String connectionString, String query, String string) {
        return "";
    }

    public String xlsToCsv(String sourcePath) throws IOException {
        String destination = sourcePath + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                destination), Charsets.UTF_8.name()))) {
            // Get the workbook object for XLSX file
            try (Workbook wBook = loadWorkbook(sourcePath)) {
                // Get first sheet from the workbook
                Sheet sheet = wBook.getSheetAt(0);
                for (Row row : sheet) {

                    String separator = "";
                    // For each row, iterate through each column
                    for (Cell cell : row) {
                        switch (cell.getCellType()) {
                        case Cell.CELL_TYPE_BOOLEAN:
                            writer.write(separator + "\"" + cell.getBooleanCellValue() + "\"");
                            break;
                        case Cell.CELL_TYPE_NUMERIC:
                        case Cell.CELL_TYPE_FORMULA:
                            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                LocalDateTime localDate = LocalDateTime.ofInstant(cell.getDateCellValue().toInstant(), ZoneId.systemDefault());
                                writer.write(separator + "\"" + formatter.format(localDate) + "\"");
                            } else {
                                // ignore formatting and output raw values. Trim decimal places for integers
                                double value = cell.getNumericCellValue();
                                if (value == Math.floor(value)) {
                                    writer.write(separator + "\"" + (int) cell.getNumericCellValue() + "\"");
                                } else {
                                    writer.write(separator + "\"" + cell.getNumericCellValue() + "\"");
                                }
                            }
                            break;
                        case Cell.CELL_TYPE_STRING:
                            writer.write(separator + "\"" + cell.getStringCellValue().replace("\"", "\"\"").trim() + "\"");
                            break;

                        case Cell.CELL_TYPE_BLANK:
                            writer.write(separator + "\"\"");
                            break;
                        default:
                            writer.write(separator + "\"" + cell.getStringCellValue().replace("\"", "\"\"").trim() + "\"");

                        }
                        separator = ",";
                    }
                    // Append new line at the end of each row
                    writer.write(System.lineSeparator());
                }
            }
        }

        return destination;
    }

    private Workbook loadWorkbook(String sourcePath) throws IOException, FileNotFoundException {
        if (sourcePath.endsWith(".xlsx")) {
            return new XSSFWorkbook(new FileInputStream(sourcePath));
        } else if (sourcePath.endsWith(".xls")) {
            return new HSSFWorkbook(new FileInputStream(sourcePath));
        } else {
            throw new IllegalArgumentException("Format of file " + sourcePath + " not supported"); 
        }
    }
}
