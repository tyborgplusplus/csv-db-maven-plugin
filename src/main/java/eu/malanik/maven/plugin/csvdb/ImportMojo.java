package eu.malanik.maven.plugin.csvdb;

import eu.malanik.maven.plugin.csvdb.csv.CsvReader;
import eu.malanik.maven.plugin.csvdb.db.DbWriter;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Mojo(name = "import")
public class ImportMojo extends AbstractMojo {

    @Parameter(property = "csvDirectory", required = true)
    private File csvDirectory;

    @Parameter(property = "dateFormat", defaultValue = "dd.MM.yyyy")
    private String dateFormat;

    @Parameter(property = "timestampFormat", defaultValue = "dd.MM.yyyy HH:mm:ss")
    private String timestampFormat;

    @Parameter(property = "dbUrl", required = true)
    private String dbUrl;

    @Parameter(property = "dbUser", required = true)
    private String dbUser;

    @Parameter(property = "dbPassword", required = true)
    private String dbPassword;

    @Parameter(property = "dbDriver", required = true)
    private String dbDriver;

    @Parameter(property = "dbSchema")
    private String dbSchema;

    @Parameter(property = "filters")
    private Set<String> filters;

    public void execute() throws MojoExecutionException {
        getLog().info("Reading data from directory " + this.csvDirectory.getAbsolutePath());
        if (this.csvDirectory.isFile()) {
            getLog().error(this.csvDirectory.getAbsolutePath() + " is not a directory");
            return;
        }

        CsvReader csvReader = new CsvReader(this.csvDirectory.toPath(), getLog());

        DbWriter dbWriter = new DbWriter(this.dbUrl, this.dbUser, this.dbPassword, this.dbDriver, this.dbSchema);

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>(0);
        if (!filters.isEmpty()) {
            try {
                Set<String> tableNames = csvReader.extractTableNames();
                getLog().info("Scanning for primary keys in tables " + tableNames);
                primaryKeyColumnsByTableName = dbWriter.determinePrimaryKeyColumns(tableNames);
                getLog().info("Primary keys scanned");
            } catch (Exception ex) {
                getLog().error(ex);
            }
        }

        Map<String, TableData> data;
        try {
            data = csvReader.readData(filters, primaryKeyColumnsByTableName);
        } catch (IOException ex) {
            getLog().error(ex);
            return;
        }

        if (data.isEmpty()) {
            getLog().warn("No suitable data found in directory " + this.csvDirectory.getAbsolutePath());
            return;
        } else {
            getLog().info(data.size() + " table data successfully loaded");
        }

        getLog().info("Importing data to database " + this.dbUrl);
        try {
            dbWriter.writeToDb(data, this.dateFormat, this.timestampFormat);
            getLog().info("Data successfully imported");
        } catch (Exception ex) {
            getLog().error(ex);
        }
    }

}
