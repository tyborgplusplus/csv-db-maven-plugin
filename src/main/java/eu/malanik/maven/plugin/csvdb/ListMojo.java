package eu.malanik.maven.plugin.csvdb;

import eu.malanik.maven.plugin.csvdb.csv.CsvReader;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Mojo(name = "list")
public class ListMojo extends AbstractMojo {

    @Parameter(property = "csvDirectory", required = true)
    private File csvDirectory;

    public void execute() throws MojoExecutionException {
        CsvReader csvReader = new CsvReader(this.csvDirectory.toPath(), getLog());
        Map<String, TableData> dataByTableName;
        try {
            dataByTableName = csvReader.readData(new HashSet<>(0), new HashMap<>(0));

            dataByTableName.forEach((k, v) -> {
                getLog().info("Table " + k + ", columns " + v.getColumnNames() + ", rows count " + v.getRows().size());
            });
        } catch (IOException ex) {
            getLog().error(ex);
        }

    }
}
