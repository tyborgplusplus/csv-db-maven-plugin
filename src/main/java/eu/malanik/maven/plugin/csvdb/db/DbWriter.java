package eu.malanik.maven.plugin.csvdb.db;

import eu.malanik.maven.plugin.csvdb.TableData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern(timestampFormat).withZone(ZoneId.systemDefault());

        for (Map.Entry<String, TableData> tableDataByTableName : data.entrySet()) {
            String tableName = tableDataByTableName.getKey();
            TableData tableData = tableDataByTableName.getValue();

            Statement deleteStatement = connection.createStatement();
            deleteStatement.executeUpdate("DELETE FROM " + tableName);
            deleteStatement.close();

            Map<String, Integer> columnTypeByName = this.determineColumnTypes(tableName, connection);

            for (TableData.Row row : tableData.getRows()) {
                String insertSql = DbWriter.INSERT_SQL.replace("$table", tableName);

                String joinedColumns = String.join(",", row.getValuesByColumnName().keySet());
                insertSql = insertSql.replace("$columns", joinedColumns);

                String joinedPlaceholders = String.join(",", Collections.nCopies(row.getValuesByColumnName().keySet().size(), "?"));
                insertSql = insertSql.replace("$values", joinedPlaceholders);

                PreparedStatement statement = connection.prepareStatement(insertSql);

                int parameterIndex = 0;
                for (String columnName : row.getValuesByColumnName().keySet()) {
                    parameterIndex++;
                    String value = row.getValuesByColumnName().get(columnName);
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
                            Instant date;
                            if ("now".equals(value)) {
                                date = Instant.now();
                            } else {
                                date = dateFormatter.parse(value, Instant::from);
                            }
                            statement.setDate(parameterIndex, new Date(date.toEpochMilli()));
                            break;
                        case Types.TIMESTAMP:
                            Instant timestamp;
                            if ("now".equals(value)) {
                                timestamp = Instant.now();
                            } else {
                                timestamp = timestampFormatter.parse(value, Instant::from);
                            }
                            statement.setTimestamp(parameterIndex, new Timestamp(timestamp.toEpochMilli()));
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

    public Map<String, Set<String>> determinePrimaryKeyColumns(Set<String> tableNames) throws Exception {
        Map<String, Set<String>> primaryKeysByTableName = new HashMap<>();

        Connection connection = this.createConnection();

        if (this.schema != null) {
            connection.setSchema(this.schema);
        }
        DatabaseMetaData meta = connection.getMetaData();

        for (String tableName : tableNames) {
            Set<String> primaryKeys = new HashSet<>();
            ResultSet rs = meta.getPrimaryKeys(null, this.schema, tableName.toUpperCase());
            while (rs.next()) {
                String primaryKeyColumn = rs.getString("COLUMN_NAME");
                primaryKeys.add(primaryKeyColumn.toLowerCase());
            }
            primaryKeysByTableName.put(tableName, primaryKeys);
        }

        connection.close();

        return primaryKeysByTableName;
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

}
