package bg.government.opendatapusher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.core.type.TypeReference;
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

    private PushConfig config;

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        List<PushConfig> configs = mapper.readValue("", new TypeReference<List<PushConfig>>() {
        });
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        for (PushConfig config : configs) {
            executor.scheduleAtFixedRate(new Pusher(config), 0, 12, TimeUnit.HOURS);
        }
    }

    public Pusher(PushConfig config) {
        this.config = config;
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
                    resultPath = xlsxToCsv(config.getPath());
                    break;
                case SQL:
                    resultPath = sqlToCsv(config.getConnectionString(), config.getQuery(), config.getPath());
                    break;
                case RAW:
                    resultPath = config.getPath();
                    break;
                }
            }

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create file", e);
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

    public String xlsxToCsv(String sourcePath) throws IOException {

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                sourcePath + ".csv"), Charsets.UTF_8.name()))) {
            // Get the workbook object for XLSX file
            try (XSSFWorkbook wBook = new XSSFWorkbook(new FileInputStream(sourcePath))) {
                // Get first sheet from the workbook
                XSSFSheet sheet = wBook.getSheetAt(0);
                for (Row row : sheet) {

                    // For each row, iterate through each column
                    for (Cell cell : row) {
                        switch (cell.getCellType()) {
                        case Cell.CELL_TYPE_BOOLEAN:
                            writer.write("\"" + cell.getBooleanCellValue() + "\",");
                            break;
                        case Cell.CELL_TYPE_NUMERIC:
                        case Cell.CELL_TYPE_FORMULA:
                            if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                writer.write("\"" + cell.getDateCellValue() + "\",");
                            } else {
                                writer.write("\"" + cell.getNumericCellValue() + "\",");
                            }
                            break;
                        case Cell.CELL_TYPE_STRING:
                            writer.write("\"" + cell.getStringCellValue().replace("\"", "\"\"") + "\",");
                            break;

                        case Cell.CELL_TYPE_BLANK:
                            writer.write("\"\"" + ",");
                            break;
                        default:
                            writer.write("\"" + cell.getStringCellValue().replace("\"", "\"\"") + "\",");

                        }
                    }
                }
            }
        }

        return "";
    }
}
