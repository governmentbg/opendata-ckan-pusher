package bg.government.opendatapusher;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Hello world!
 *
 */
public class Pusher {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    }

    public void xlsxToCsv(String sourcePath) {
        // For storing data into CSV files
        StringBuffer data = new StringBuffer();

        try {
            // Get the workbook object for XLSX file
            XSSFWorkbook wBook = new XSSFWorkbook(new FileInputStream(sourcePath));
            // Get first sheet from the workbook
            XSSFSheet sheet = wBook.getSheetAt(0);
            // Iterate through each rows from first sheet
            Iterator<Row> rowIterator = sheet.iterator();

            for (Row row : sheet) {

                // For each row, iterate through each column
                for (Cell cell : row) {
                    switch (cell.getCellType()) {
                    case Cell.CELL_TYPE_BOOLEAN:
                        data.append(cell.getBooleanCellValue() + ",");

                        break;
                    case Cell.CELL_TYPE_NUMERIC:
                    case Cell.CELL_TYPE_FORMULA:
                        data.append(cell.getNumericCellValue() + ",");
                        break;
                    case Cell.CELL_TYPE_STRING:
                        data.append(cell.getStringCellValue() + ",");
                        break;

                    case Cell.CELL_TYPE_BLANK:
                        data.append("" + ",");
                        break;
                    default:
                        data.append(cell + ",");

                    }
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
