package eu.malanik.maven.plugin.csvdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableData {

    private Map<String, List<String>> rowValuesByColumnName = new HashMap<>();

    public Map<String, List<String>> getRowValuesByColumnName() {
        return rowValuesByColumnName;
    }

}
