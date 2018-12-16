package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.FileNameData;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FileNameParser {

    public static FileNameData parseCsvFileName(String fileName) {
        FileNameData result = new FileNameData();
        int tableEndName = fileName.indexOf("__");
        if (tableEndName != -1) {
            result.setTableName(fileName.substring(0, tableEndName).toUpperCase());

            int suffixStart = fileName.indexOf(".CSV");
            String viewsSequence = (String) fileName.subSequence(tableEndName + 2, suffixStart);
            String[] views = viewsSequence.split("_");
            result.getViews().addAll(Arrays.stream(views).map(String::toUpperCase).collect(Collectors.toSet()));
        } else {
            result.setTableName(fileName.replace(".CSV", "").toUpperCase());
        }

        return result;
    }


}
