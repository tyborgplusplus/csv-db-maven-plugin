package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.TableData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CsvReader {

    private File csvDirectory;

    public CsvReader(File csvDirectory) {
        this.csvDirectory = csvDirectory;
    }

    public Map<String, TableData> readData() throws IOException {
        Map<String, TableData> dataByTableName = new HashMap<>();

        File[] csvFiles = this.csvDirectory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (csvFiles == null || csvFiles.length == 0) {
            return dataByTableName;
        }

        for (File file : csvFiles) {
            String tableName = this.extractTableName(file.getName());

            TableData tableData = dataByTableName.get(tableName);
            if (tableData == null) {
                tableData = new TableData();
                dataByTableName.put(tableName, tableData);
            }

            Reader in = new FileReader(file);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(in);

            Set<String> columns = ((CSVParser) records).getHeaderMap().keySet();
            for (String columnName : columns) {
                if (!tableData.getDataByColumnName().containsKey(columnName)) {
                    tableData.getDataByColumnName().put(columnName.toLowerCase(), new ArrayList<>());
                }
            }

            for (CSVRecord record : records) {
                for (String column : columns) {
                    List<String> values = tableData.getDataByColumnName().get(column);
                    String value = record.get(column);
                    values.add(value);
                }
            }
        }

        return dataByTableName;
    }

    private String extractTableName(String fileName) {
        int tableEndName = fileName.indexOf("__");
        if (tableEndName != -1) {
            return fileName.substring(0, tableEndName);
        } else {
            return fileName.replace(".csv", "");
        }
    }

}
