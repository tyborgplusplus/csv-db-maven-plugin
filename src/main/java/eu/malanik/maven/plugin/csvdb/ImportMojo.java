package eu.malanik.maven.plugin.csvdb;

import eu.malanik.maven.plugin.csvdb.db.DatabaseConnector;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

import java.util.Map;

@Mojo(name = "import")
public class ImportMojo extends BaseMojo {

    public void execute() throws MojoExecutionException {

        Map<String, TableData> dataByTableName = this.prepareData();

        if (dataByTableName.isEmpty()) {
            getLog().warn("No suitable data found in directory " + this.csvDirectory.getAbsolutePath());
            return;
        } else {
            dataByTableName.forEach((k, v) -> getLog().info("Table " + k + " with " + v.getRows().size() + " rows successfully loaded"));
        }

        DatabaseConnector databaseConnector = new DatabaseConnector(this.dbUrl, this.dbUser, this.dbPassword, this.dbDriver, this.dbSchema);

        getLog().info("Writing data to database " + this.dbUrl);
        try {
            databaseConnector.writeToDb(dataByTableName, this.dateFormat, this.timestampFormat);
            getLog().info("Data successfully written");
        } catch (Exception ex) {
            getLog().error(ex);
            throw new MojoExecutionException(ex.getMessage());
        }
    }

}
