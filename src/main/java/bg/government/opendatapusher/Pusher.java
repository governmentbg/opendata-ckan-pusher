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
    
    private PushConfig config;
    private String apiKey;
    private String rootUrl;
    
    private RestTemplate restTemplate = new RestTemplate();
    
    public static void main(String[] args) throws Exception {
        ConfigRoot configRoot = parseConfig(args[0]);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        for (PushConfig config : configRoot.getConfigs()) {
            executor.scheduleAtFixedRate(new Pusher(config, configRoot.getApiKey(), configRoot.getRootUrl()), 0, 12, TimeUnit.HOURS);
        }
    }

    public static ConfigRoot parseConfig(String path) throws IOException, JsonParseException,
            JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ConfigRoot config = mapper.readValue(new File(path), ConfigRoot.class);
        return config;
    }

    public Pusher(PushConfig config, String apiKey, String rootUrl) {
        this.config = config;
        this.apiKey = apiKey;
        this.rootUrl = rootUrl;
    }

    public void run() {
        try {
            File file = getOrCreateFile();
            long lastRunMillis = Long.parseLong(Files.readFirstLine(file, Charsets.UTF_8));
            LocalDateTime lastRun = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastRunMillis),
                    ZoneId.systemDefault());
            if (lastRun.isBefore(LocalDateTime.now().minusDays(config.getDays()))) {
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
                
                pushDataset(resultPath);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create file", e);
        }
    }

    private void pushDataset(String csvPath) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", apiKey);
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        
        HttpEntity<?> request = new  HttpEntity<>(headers);
        MultiValueMap<String, Object> map = new LinkedMultiValueMap<String, Object>();
        map.add("resource_id", config.getResourceKey());
        map.add("upload", new FileSystemResource(csvPath));
        
        try {
            String result = restTemplate.exchange(new URI(rootUrl + UPDATE_OPERATION), HttpMethod.POST, request, String.class).getBody();
            logger.info("Result: " + result);
        } catch (RestClientException | URISyntaxException e) {
            throw new IOException(e);
        }
        
    }

    private File getOrCreateFile() throws IOException {
        File file = new File(config.getTitle() + ".last");
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
