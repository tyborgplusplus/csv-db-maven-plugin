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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CsvReader {

    private Path csvDirectory;

    private Log log;

    public CsvReader(Path csvDirectory, Log log) {
        Objects.requireNonNull(csvDirectory);
        Objects.requireNonNull(log);
        this.csvDirectory = csvDirectory;
        this.log = log;
    }

    public Map<String, TableData> readData(Set<String> configuredFilters, Map<String, Set<String>> primaryKeyColumnsByTableName) throws IOException {
        Map<String, TableData> dataByTableName = new HashMap<>();

        List<File> csvFiles = searchFiles();

        if (csvFiles.isEmpty()) {
            return dataByTableName;
        }

        for (File file : csvFiles) {
            FileNameData fileNameData = FileNameParser.parseCsvFileName(file.getName().toLowerCase());
            String tableName = fileNameData.getTableName();

            Set<String> nameFilters = fileNameData.getFilters();
            Set<String> copyOfNameFilters = new HashSet<>(nameFilters);
            copyOfNameFilters.removeAll(configuredFilters);
            if (!copyOfNameFilters.isEmpty()) {
                log.info(file.getName() + " ignored because of configured filters: " + configuredFilters);
                continue;
            }

            TableData tableData = dataByTableName.get(tableName);
            if (tableData == null) {
                tableData = new TableData();
                dataByTableName.put(tableName, tableData);
            }
            Set<String> primaryKeyColumns = primaryKeyColumnsByTableName.get(tableName);

            CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(file));

            Set<String> columns = records.getHeaderMap().keySet();
            for (String columnName : columns) {
                tableData.getColumnNames().add(columnName.toLowerCase());
            }

            for (CSVRecord record : records) {
                TableData.Row row = new TableData.Row();

                StringBuilder rowId = new StringBuilder();
                for (String column : columns) {
                    String valueFromFile = record.get(column);

                    if (primaryKeyColumns != null && primaryKeyColumns.contains(column)) {
                        rowId.append(valueFromFile);
                    }

                    row.getValuesByColumnName().put(column, !valueFromFile.isEmpty() ? valueFromFile : null);
                }

                row.setId(rowId.length() > 0 ? rowId.toString() : null);

                row.getFilters().addAll(nameFilters);

                TableData.Row existingRow = tableData.getById(row.getId());
                if (existingRow != null) {
                    int existingRowFilterImportance = existingRow.getFilters().size();
                    int fileRowFilterImportance = nameFilters.size();
                    if (existingRowFilterImportance < fileRowFilterImportance) {
                        tableData.getRows().remove(existingRow);
                    } else if (existingRowFilterImportance == fileRowFilterImportance) {
                        log.warn("Same importance of " + existingRow + " vs. " + row);
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
            FileNameData fileNameData = FileNameParser.parseCsvFileName(file.getName().toLowerCase());
            result.add(fileNameData.getTableName());
        }

        return result;
    }

    private List<File> searchFiles() throws IOException {
        return Files.walk(csvDirectory)
            .filter(Files::isRegularFile)
            .map(Path::toFile)
            .filter(f -> f.getName().toLowerCase().endsWith(".csv"))
            .sorted()
            .collect(Collectors.toList());
    }

}
