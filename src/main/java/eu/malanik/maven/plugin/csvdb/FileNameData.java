package eu.malanik.maven.plugin.csvdb;

import java.util.HashSet;
import java.util.Set;

public class FileNameData {

    private String tableName;

    private Set<String> views = new HashSet<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Set<String> getViews() {
        return views;
    }

}
