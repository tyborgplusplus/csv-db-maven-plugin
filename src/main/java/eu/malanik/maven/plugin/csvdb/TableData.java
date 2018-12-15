package eu.malanik.maven.plugin.csvdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TableData {

    private List<String> columnNames = new ArrayList<>();

    private List<Row> rows = new ArrayList<>();

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Row> getRows() {
        return rows;
    }

    public Row getById(String id) {
        if (id == null) {
            return null;
        }

        Optional<Row> existingRow = rows.stream().filter(r -> id.equals(r.getId())).findFirst();
        return existingRow.orElse(null);
    }

    public static class Row {

        private String id;

        private Map<String, String> valuesByColumnName = new HashMap<>(0);

        private Set<String> filters = new HashSet<>(0);

        public Row() {
        }

        public Row(String id, Map<String, String> valuesByColumnName) {
            this.id = id;
            this.valuesByColumnName = valuesByColumnName;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Map<String, String> getValuesByColumnName() {
            return valuesByColumnName;
        }

        public Set<String> getFilters() {
            return filters;
        }

        @Override
        public String toString() {
            return "Row { id:'" + id + ", valuesByColumnName:" + valuesByColumnName + ", filters:" + filters + " }";
        }
    }

}
