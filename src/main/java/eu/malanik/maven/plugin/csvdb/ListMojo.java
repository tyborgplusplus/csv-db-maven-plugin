package eu.malanik.maven.plugin.csvdb;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

import java.util.Map;

@Mojo(name = "list")
public class ListMojo extends BaseMojo {

    public void execute() throws MojoExecutionException {
        Map<String, TableData> dataByTableName = this.prepareData();

        getLog().info("");
        if (dataByTableName.isEmpty()) {
            getLog().warn("No suitable data found in directory " + this.csvDirectory.getAbsolutePath());
        } else {
            int columnSize = 32;
            int leftBorderSize = 2;
            int columnContentSize = columnSize - leftBorderSize;
            String columnFormat = "| %-" + columnContentSize + "s";

            dataByTableName.forEach((k, v) -> {
                int tableSize = (columnSize * v.getColumnNames().size()) - leftBorderSize;

                StringBuilder horizontalLineSb = new StringBuilder("+-");
                for (int i = 0; i < tableSize; i++) {
                    horizontalLineSb.append('-');
                }
                horizontalLineSb.append("+");
                String horizontalLine = horizontalLineSb.toString();

                getLog().info(horizontalLine);
                getLog().info(String.format(("| %-" + tableSize + "s|"), k.toUpperCase()));

                getLog().info(horizontalLine);

                StringBuilder columns = new StringBuilder();
                v.getColumnNames().forEach(c -> {
                    columns.append(String.format(columnFormat, c));
                });
                columns.append("|");
                getLog().info(columns.toString());

                getLog().info(horizontalLine);

                for (TableData.Row row : v.getRows()) {
                    StringBuilder rowSb = new StringBuilder();
                    for (String column : v.getColumnNames()) {
                        rowSb.append(String.format(columnFormat, row.getValuesByColumnName().getOrDefault(column, "")));
                    }
                    rowSb.append("|");
                    getLog().info(rowSb.toString());

                }

                getLog().info(horizontalLine);
                getLog().info("");
            });
        }

    }
}
