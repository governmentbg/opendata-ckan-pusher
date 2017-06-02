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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    
    private static final String PREFIX = "/api/3/action/";
    private static final String UPDATE_OPERATION = PREFIX + "resource_update";
    private static final String CREATE_OPERATION = PREFIX + "resource_create";
    private static final String RESOURCE_SHOW_OPERATION = PREFIX + "resource_show";

    private static final String DEFAULT_API_KEY = "xxxxxx";
    
    private String path;
    
    private String apiKey;
    private String rootUrl;
    
    private RestTemplate restTemplate = new RestTemplate();
    
    private ObjectMapper objectMapper = new ObjectMapper();
    
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
                    LocalDateTime lastConfigModified = LocalDateTime.ofInstant(Instant.ofEpochMilli(new File(path).lastModified()), ZoneId.systemDefault());
                    if (lastRun.isBefore(LocalDateTime.now().minusDays(config.getDays()))
                            || (config.isPushAfterConfigChange() && lastRun.isBefore(lastConfigModified))) {
                        String resultPath = null;
                        switch (config.getSourceType()) {
                        case XLS:
                            resultPath = xlsToCsv(config.getPath(), config.isUseBom());
                            break;
                        case SQL:
                            resultPath = sqlToCsv(config.getConnectionString(), config.getQuery(), config.getPath(), config.isUseBom());
                            break;
                        case RAW:
                            resultPath = config.getPath();
                            break;
                        }
                        logger.info("Parsing complete; pushing to opendata portal");
                        
                        pushDataset(resultPath, config.getResourceKey(), config.isAppend());
                        storeSuccessfulRun(file);
                    } else {
                        logger.info("Skipping run; last run was too recently: " + lastRun + ". File last modified: " + lastConfigModified);
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

    private void pushDataset(String csvPath, String resourceKey, boolean append) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        
        HttpHeaders jsonHeaders = new HttpHeaders();
        jsonHeaders.set("Authorization", apiKey);
        jsonHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
        
        HttpEntity<?> request = new  HttpEntity<>(headers);
        
        try {
            if (append) {
                String url = rootUrl + RESOURCE_SHOW_OPERATION;
                String result = restTemplate.exchange(new URI(url), HttpMethod.GET, request, String.class).getBody();
                JsonNode node = objectMapper.readTree(result).get("result"); 
                String packageId = node.get("package_id").asText();
                String name = node.get("name") + formatter.format(LocalDateTime.now());
                
                ObjectNode requestNode = objectMapper.createObjectNode();
                requestNode.put("package_id", packageId);
                requestNode.put("name", name);
                requestNode.put("url", "dummy");
                HttpEntity<?> createRequest = new  HttpEntity<>(requestNode, jsonHeaders);
                
                url = rootUrl + CREATE_OPERATION;
                String createResult = restTemplate.exchange(new URI(url), HttpMethod.GET, createRequest, String.class).getBody();
                resourceKey = objectMapper.readTree(createResult).get("result").get("id").asText();
            }
            
            MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
            map.add("id", resourceKey);
            map.add("upload", new FileSystemResource(csvPath));
            
            HttpEntity<?> updateRequest = new  HttpEntity<>(map, headers);
            
            String url = rootUrl + UPDATE_OPERATION;
            String result = restTemplate.exchange(new URI(url), HttpMethod.POST, updateRequest, String.class).getBody();
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

    public String sqlToCsv(String connectionString, String query, String dir, boolean useBom) {
        try {
            Connection conn = DriverManager.getConnection(connectionString);
            ResultSet rs = conn.createStatement().executeQuery(query);
            String destination = dir + "/sqlexport.csv";
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    destination), Charsets.UTF_8.name()))) {
                if (useBom) {
                    writer.write('\ufeff');
                }
                while (rs.next()) {
                    String separator = "";
                    for (int i = 0; i < rs.getMetaData().getColumnCount(); i ++) {
                        writer.write(separator + "\"" + rs.getObject(i).toString() + "\"");
                        separator = ",";
                    }
                    // Append new line at the end of each row
                    writer.write(System.lineSeparator());
                }
            }
            return destination;
        } catch (IOException | SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String xlsToCsv(String sourcePath, boolean useBom) throws IOException {
        String destination = sourcePath + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                destination), Charsets.UTF_8.name()))) {
            if (useBom) {
                writer.write('\ufeff');
            }
            // Get the workbook object for XLSX file
            try (Workbook wBook = loadWorkbook(sourcePath)) {
                // Get first sheet from the workbook
                Sheet sheet = wBook.getSheetAt(0);
                for (Row row : sheet) {
                    boolean hasContent = false;
                    String separator = "";
                    // For each row, iterate through each column
                    for (Cell cell : row) {
                        switch (cell.getCellType()) {
                        case Cell.CELL_TYPE_BOOLEAN:
                            writer.write(separator + "\"" + cell.getBooleanCellValue() + "\"");
                            hasContent = true;
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
                            hasContent = true;
                            break;
                        case Cell.CELL_TYPE_STRING:
                            writer.write(separator + "\"" + cell.getStringCellValue().replace("\"", "\"\"").trim() + "\"");
                            hasContent = true;
                            break;

                        case Cell.CELL_TYPE_BLANK:
                            writer.write(separator + "\"\"");
                            break;
                        default:
                            writer.write(separator + "\"" + cell.getStringCellValue().replace("\"", "\"\"").trim() + "\"");
                            hasContent = true;

                        }
                        separator = ",";
                    }
                    // Append new line at the end of each row
                    if (hasContent) {
                        writer.write(System.lineSeparator());
                    }
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
