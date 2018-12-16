package eu.malanik.maven.plugin.csvdb;

import eu.malanik.maven.plugin.csvdb.csv.CsvReader;
import eu.malanik.maven.plugin.csvdb.db.DatabaseConnector;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseMojo extends AbstractMojo {

    @Parameter(property = "csvDirectory", required = true)
    protected File csvDirectory;

    @Parameter(property = "csvDelimiter", defaultValue = ",")
    protected char csvDelimiter;

    @Parameter(property = "dateFormat", defaultValue = "dd.MM.yyyy")
    protected String dateFormat;

    @Parameter(property = "timestampFormat", defaultValue = "dd.MM.yyyy HH:mm:ss")
    protected String timestampFormat;

    @Parameter(property = "dbUrl", required = true)
    protected String dbUrl;

    @Parameter(property = "dbUser", required = true)
    protected String dbUser;

    @Parameter(property = "dbPassword", required = true)
    protected String dbPassword;

    @Parameter(property = "dbDriver", required = true)
    protected String dbDriver;

    @Parameter(property = "dbSchema")
    protected String dbSchema;

    @Parameter(property = "views")
    protected List<String> views;

    protected Map<String, TableData> prepareData() throws MojoExecutionException {
        getLog().info("Reading data from directory " + this.csvDirectory.getAbsolutePath());

        CsvReader csvReader = new CsvReader(this.csvDirectory.toPath(), this.csvDelimiter, getLog());

        DatabaseConnector databaseConnector = new DatabaseConnector(this.dbUrl, this.dbUser, this.dbPassword, this.dbDriver, this.dbSchema);

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>(0);
        views.replaceAll(String::toUpperCase);
        if (!views.isEmpty()) {
            try {
                Set<String> tableNames = csvReader.extractTableNames();
                getLog().info("Scanning for primary keys in tables " + tableNames);
                primaryKeyColumnsByTableName = databaseConnector.determinePrimaryKeyColumns(tableNames);
                getLog().info("Primary keys found " + primaryKeyColumnsByTableName);
            } catch (Exception ex) {
                getLog().error(ex);
                throw new MojoExecutionException(ex.getMessage());
            }
        }

        Map<String, TableData> dataByTableName;
        try {
            dataByTableName = csvReader.readData(new HashSet<>(views), primaryKeyColumnsByTableName);
        } catch (IOException ex) {
            getLog().error(ex);
            throw new MojoExecutionException(ex.getMessage());
        }

        return dataByTableName;
    }


}
