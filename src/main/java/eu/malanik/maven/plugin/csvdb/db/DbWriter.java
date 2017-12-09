package eu.malanik.maven.plugin.csvdb.db;

import eu.malanik.maven.plugin.csvdb.TableData;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    public void writeToDb(Map<String, TableData> data, final String dateFormat, final String timestampFormat)
            throws Exception {
        Connection connection = this.createConnection();

        if (this.schema != null) {
            connection.setSchema(this.schema);
        }

        for (Map.Entry<String, TableData> entry : data.entrySet()) {
            String tableName = entry.getKey();
            List<String> columnNames = new ArrayList<>(entry.getValue().getRowValuesByColumnName().keySet());

            Statement deleteStatement = connection.createStatement();
            deleteStatement.executeUpdate("DELETE FROM " + tableName);
            deleteStatement.close();

            Map<String, Integer> columnTypeByName = this.determineColumnTypes(tableName, connection);

            for (int rowIndex = 0; rowIndex < this.getRowCount(entry.getValue()); rowIndex++) {
                String insertSql = DbWriter.INSERT_SQL.replace("$table", tableName);

                List<String> filledColumns = this.determineFilledColumns(entry.getValue(), rowIndex);
                if (filledColumns.isEmpty()) {
                    // row is empty
                    continue;
                }

                String joinedColumns = String.join(",", filledColumns);
                insertSql = insertSql.replace("$columns", joinedColumns);

                String joinedPlaceholders = String.join(",", Collections.nCopies(filledColumns.size(), "?"));
                insertSql = insertSql.replace("$values", joinedPlaceholders);

                PreparedStatement statement = connection.prepareStatement(insertSql);

                int parameterIndex = 0;
                for (int columnNameIndex = 0; columnNameIndex < columnNames.size(); columnNameIndex++) {
                    String columnName = columnNames.get(columnNameIndex);
                    if (!filledColumns.contains(columnName)) {
                        // column name null in this row
                        continue;
                    }
                    parameterIndex++;
                    List<String> columnValues = entry.getValue().getRowValuesByColumnName().get(columnName);
                    String value = columnValues.get(rowIndex);
                    Integer columnType = columnTypeByName.get(columnName);
                    if (columnType == null) {
                        throw new IllegalArgumentException("Column " + columnName + " does not exists in table " + tableName);
                    }
                    switch(columnType) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                            statement.setString(parameterIndex, value);
                            break;
                        case Types.DOUBLE:
                            statement.setDouble(parameterIndex, Double.valueOf(value));
                            break;
                        case Types.INTEGER:
                            statement.setInt(parameterIndex, Integer.valueOf(value));
                            break;
                        case Types.BIGINT:
                            statement.setLong(parameterIndex, Long.valueOf(value));
                            break;
                        case Types.BOOLEAN:
                            statement.setBoolean(parameterIndex, Boolean.valueOf(value));
                            break;
                        case Types.DATE:
                            java.util.Date date;
                            if ("now".equals(value)) {
                                date = new java.util.Date();
                            } else {
                                date = new SimpleDateFormat(dateFormat).parse(value);
                            }
                            statement.setDate(parameterIndex, new Date(date.getTime()));
                            break;
                        case Types.TIMESTAMP:
                            java.util.Date timestamp;
                            if ("now".equals(value)) {
                                timestamp = new java.util.Date();
                            } else {
                                timestamp = new SimpleDateFormat(timestampFormat).parse(value);
                            }
                            statement.setTimestamp(parameterIndex, new Timestamp(timestamp.getTime()));
                            break;
                        default:
                            throw new IllegalAccessException(
                                    columnName + " has not supported column type " + columnType);
                    }
                }

                statement.executeUpdate();
                statement.close();
            }
        }
        connection.close();
    }

    private Map<String, Integer> determineColumnTypes(final String tableName, final Connection connection)
            throws SQLException {
        Map<String, Integer> columnTypeByName = new HashMap<>();

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i).toLowerCase();
            int columnType = metaData.getColumnType(i);
            columnTypeByName.put(columnName, columnType);
        }

        return columnTypeByName;
    }

    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName(this.dbDriver);
        return DriverManager.getConnection(this.url, this.user, this.password);
    }

    private int getRowCount(TableData data) {
        if (data.getRowValuesByColumnName().keySet().isEmpty()) {
            // empty
            return 0;
        } else {
            List<String> columns = new ArrayList<>(data.getRowValuesByColumnName().keySet());
            // every column has the same count of rows
            return data.getRowValuesByColumnName().get(columns.get(0)).size();
        }
    }

    private List<String> determineFilledColumns(final TableData tableData, final int rowIndex) {
        List<String> filledColumns = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : tableData.getRowValuesByColumnName().entrySet()) {

            String rowValueInColumn = entry.getValue().get(rowIndex);
            if (rowValueInColumn != null && !rowValueInColumn.isEmpty()) {
                filledColumns.add(entry.getKey());
            }
        }

        return filledColumns;
    }

}
