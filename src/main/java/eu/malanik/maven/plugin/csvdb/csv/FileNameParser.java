package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.FileNameData;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FileNameParser {

    public static FileNameData parseCsvFileName(String fileName) {
        FileNameData result = new FileNameData();
        int tableEndName = fileName.indexOf("__");
        if (tableEndName != -1) {
            result.setTableName(fileName.substring(0, tableEndName));

            int suffixStart = fileName.indexOf(".csv");
            String filtersSequence = (String) fileName.subSequence(tableEndName + 2, suffixStart);
            String[] filters = filtersSequence.split("_");
            result.getFilters().addAll(Arrays.stream(filters).map(String::toLowerCase).collect(Collectors.toSet()));
        } else {
            result.setTableName(fileName.replace(".csv", ""));
        }

        return result;
    }


}
