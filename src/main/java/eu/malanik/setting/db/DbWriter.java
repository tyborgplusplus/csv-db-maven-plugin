package eu.malanik.setting.db;

import eu.malanik.setting.TableData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DbWriter {

    private static final String INSERT_SQL = "INSERT INTO $table ($columns) VALUES ($values)";

    private String url;

    private String user;

    private String password;

    private String schema;

    private String dbDriver;

    public DbWriter(String url, String user, String password, String dbDriver, String schema) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.dbDriver = dbDriver;
        this.schema = schema;
    }

    public void writeToDb(Map<String, TableData> data) throws Exception {
        Connection connection = this.createConnection();

        if (this.schema != null) {
            connection.setSchema(this.schema);
        }

        for (Map.Entry<String, TableData> entry : data.entrySet()) {
            String tableName = entry.getKey();
            List<String> columnNames = new ArrayList<>(entry.getValue().getDataByColumnName().keySet());

            Statement deleteStatement = connection.createStatement();
            deleteStatement.executeUpdate("DELETE FROM " + tableName);
            deleteStatement.close();

            for (int rowIndex = 0; rowIndex < this.getRowCount(entry.getValue()); rowIndex++) {
                String insertSql = DbWriter.INSERT_SQL.replace("$table", tableName);

                String joinedColumns = String.join(",", columnNames);
                insertSql = insertSql.replace("$columns", joinedColumns);

                String joinedPlaceholders = String.join(",", Collections.nCopies(columnNames.size(), "?"));
                insertSql = insertSql.replace("$values", joinedPlaceholders);

                PreparedStatement statement = connection.prepareStatement(insertSql);

                for (int parameterIndex = 0; parameterIndex < columnNames.size(); parameterIndex++) {
                    String columnName = columnNames.get(parameterIndex);
                    List<String> columnValues = entry.getValue().getDataByColumnName().get(columnName);
                    String value = columnValues.get(rowIndex);
                    statement.setString((parameterIndex + 1), value);
                }

                statement.executeUpdate();
                statement.close();
            }
        }
        connection.close();
    }

    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName(this.dbDriver);
        return DriverManager.getConnection(this.url, this.user, this.password);
    }

    private int getRowCount(TableData data) {
        return data.getDataByColumnName().values().size();
    }

}
