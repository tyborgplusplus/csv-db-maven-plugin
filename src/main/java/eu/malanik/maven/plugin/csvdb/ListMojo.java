package eu.malanik.maven.plugin.csvdb;

import eu.malanik.maven.plugin.csvdb.csv.CsvReader;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Mojo(name = "list")
public class ListMojo extends AbstractMojo {

    @Parameter(property = "csvDirectory", required = true)
    private File csvDirectory;

    public void execute() throws MojoExecutionException {
        CsvReader csvReader = new CsvReader(this.csvDirectory);
        Map<String, TableData> dataByTableName;
        try {
            dataByTableName = csvReader.readData();

            dataByTableName.forEach((k, v) -> {
                getLog().info("Table " + k + ", columns " + v.getDataByColumnName().size()
                    + ", rows " + v.getDataByColumnName().values().iterator().next().size());
            });
        } catch (IOException ex) {
            getLog().error(ex);
        }

    }
}
