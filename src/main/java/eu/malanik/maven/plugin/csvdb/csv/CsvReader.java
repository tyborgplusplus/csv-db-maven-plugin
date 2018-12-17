package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.FileNameData;
import eu.malanik.maven.plugin.csvdb.TableData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.maven.plugin.logging.Log;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class CsvReader {

    private Path csvDirectory;

    private char delimiter;

    private Log log;

    public CsvReader(Path csvDirectory, Log log) {
        Objects.requireNonNull(csvDirectory);
        Objects.requireNonNull(log);
        this.csvDirectory = csvDirectory;
        this.log = log;
    }

    public CsvReader(Path csvDirectory, char delimiter, Log log) {
        Objects.requireNonNull(csvDirectory);
        Objects.requireNonNull(log);
        this.csvDirectory = csvDirectory;
        this.delimiter = delimiter;
        this.log = log;
    }

    public Map<String, TableData> readData(Set<String> configuredViews, Map<String, Set<String>> primaryKeyColumnsByTableName) throws IOException {
        Map<String, TableData> dataByTableName = new HashMap<>();

        List<File> csvFiles = searchFiles();

        if (csvFiles.isEmpty()) {
            return dataByTableName;
        }

        for (File file : csvFiles) {
            FileNameData fileNameData = FileNameParser.parseCsvFileName(file.getName().toUpperCase());
            String tableName = fileNameData.getTableName();

            Set<String> nameViews = fileNameData.getViews();
            Set<String> copyOfNameViews = new HashSet<>(nameViews);
            copyOfNameViews.removeAll(configuredViews);
            if (!copyOfNameViews.isEmpty()) {
                log.info(file.getName() + " ignored because of configured views: " + configuredViews);
                continue;
            }

            TableData tableData = dataByTableName.get(tableName);
            if (tableData == null) {
                tableData = new TableData();
                dataByTableName.put(tableName, tableData);
            }
            Set<String> primaryKeyColumns = primaryKeyColumnsByTableName.get(tableName);

            CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader();
            if (delimiter != '\u0000') { // default char
                csvFormat = csvFormat.withDelimiter(delimiter);
            }
            CSVParser records = csvFormat.parse(new FileReader(file));

            Set<String> columns = records.getHeaderMap().keySet();
            for (String columnName : columns) {
                if (!tableData.getColumnNames().contains(columnName.toUpperCase())) {
                    tableData.getColumnNames().add(columnName.toUpperCase());
                }
            }

            for (CSVRecord record : records) {
                TableData.Row row = new TableData.Row();

                StringBuilder rowId = new StringBuilder();
                for (String column : columns) {
                    String valueFromFile = record.get(column);

                    if (primaryKeyColumns != null && primaryKeyColumns.contains(column.toUpperCase())) {
                        rowId.append(valueFromFile);
                    }

                    if (valueFromFile != null && !valueFromFile.isEmpty()) {
                        row.getValuesByColumnName().put(column.toUpperCase(), valueFromFile);
                    }
                }

                row.setId(rowId.length() > 0 ? rowId.toString() : null);

                row.getViews().addAll(nameViews);

                TableData.Row existingRow = tableData.getById(row.getId());
                if (existingRow != null) {
                    int existingRowViewImportance = existingRow.getViews().size();
                    int fileRowViewImportance = nameViews.size();
                    if (existingRowViewImportance < fileRowViewImportance) {
                        tableData.getRows().remove(existingRow);
                    } else if (existingRowViewImportance == fileRowViewImportance) {
                        log.warn("Same views for " + existingRow + " and " + row + ". " + row + " will be used.");
                        tableData.getRows().remove(existingRow);
                    }
                }

                if (row.getValuesByColumnName().values().stream().anyMatch(Objects::nonNull)) {
                    // empty rows should not be added
                    tableData.getRows().add(row);
                }

            }
        }

        return dataByTableName;
    }

    public Set<String> extractTableNames() throws IOException {
        Set<String> result = new HashSet<>();

        List<File> csvFiles = searchFiles();

        if (csvFiles.isEmpty()) {
            return result;
        }

        for (File file : csvFiles) {
            FileNameData fileNameData = FileNameParser.parseCsvFileName(file.getName().toUpperCase());
            result.add(fileNameData.getTableName());
        }

        return result;
    }

    private List<File> searchFiles() throws IOException {
        if (this.csvDirectory.toFile().listFiles() == null) {
            return Collections.emptyList();
        }
        return Files.walk(csvDirectory)
            .filter(Files::isRegularFile)
            .map(Path::toFile)
            .filter(f -> f.getName().toUpperCase().endsWith(".CSV"))
            .sorted()
            .collect(Collectors.toList());
    }

}
