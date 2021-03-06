package eu.malanik.maven.plugin.csvdb.db;

import eu.malanik.maven.plugin.csvdb.TableData;
import org.h2.util.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatabaseConnectorTest {

    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static final String DB_URL = "jdbc:h2:./target/DbWriterTest;TRACE_LEVEL_SYSTEM_OUT=2";

    private static final String DB_USER = "sa";

    private static final String DB_PASSWORD = "";

    private static final String DATE_FORMAT = "dd.MM.yyyy";

    private static final String TIMESTAMP_FORMAT = "dd.MM.yyyy HH:mm:ss";

    private Connection connection = null;

    @BeforeEach
    public void setUp() throws SQLException, ClassNotFoundException {
        Statement statement;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS SETTING");
            statement.executeUpdate("CREATE TABLE SETTING ("
                + "STRING VARCHAR(30) PRIMARY KEY,"
                + "INTEGER INT,"
                + "DOUBLE DOUBLE,"
                + "BOOLEAN BOOLEAN,"
                + "DATE DATE,"
                + "TIMESTAMP TIMESTAMP,"
                + "EMPTY VARCHAR(30),"
                + "NULLL VARCHAR(30))");

            statement.close();
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void writeToDb() throws Exception {
        DatabaseConnector underTest = new DatabaseConnector(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        tableData.getColumnNames().addAll(new HashSet<>(Arrays.asList("STRING", "INTEGER", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "EMPTY", "NULLL")));
        tableData.getRows().add(new TableData.Row("string", new HashMap<String, String>()
        {{
            put("STRING", "text");
            put("INTEGER", "5");
            put("DOUBLE", "3.14");
            put("BOOLEAN", "true");
            put("DATE", "now");
            put("TIMESTAMP", "01.07.2009 19:23:55");
            put("EMPTY", "");
            put("NULLL", null);
        }}));
        data.put("SETTING", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
                Assertions.assertEquals("text", resultSet.getString("STRING"));

                Assertions.assertEquals(5, resultSet.getInt("INTEGER"));

                Assertions.assertEquals(BigDecimal.valueOf(3.14), BigDecimal.valueOf(resultSet.getDouble("DOUBLE")));

                Assertions.assertEquals(Boolean.TRUE, resultSet.getBoolean("BOOLEAN"));

                Assertions.assertEquals(LocalDate.now(), resultSet.getDate("DATE").toLocalDate());

                Assertions.assertEquals(
                        LocalDateTime.parse("01.07.2009 19:23:55", DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT)),
                        resultSet.getTimestamp("TIMESTAMP").toLocalDateTime());

                Assertions.assertEquals("", resultSet.getString("EMPTY"));

                Assertions.assertNull(resultSet.getString("NULLL"));
            }
            Assertions.assertEquals(1, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }

    @Test
    public void writeToDbNoRows() throws Exception {
        DatabaseConnector underTest = new DatabaseConnector(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        tableData.getColumnNames().addAll(new HashSet<>(Arrays.asList("KEY", "VALUE")));
        data.put("SETTING", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
            }
            Assertions.assertEquals(0, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }

    @Test
    public void writeToDbNoColumns() throws Exception {
        DatabaseConnector underTest = new DatabaseConnector(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        data.put("SETTING", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
            }
            Assertions.assertEquals(0, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }


    @Test
    public void determinePrimaryKeyColumns() throws Exception {
        DatabaseConnector underTest = new DatabaseConnector(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);
        String tableName = "SETTING";

        Map<String, Set<String>> result = underTest.determinePrimaryKeyColumns(new HashSet<>(Collections.singletonList(tableName)));

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get(tableName));
        Assertions.assertEquals(1, result.get(tableName).size());
        Assertions.assertEquals("STRING", result.get(tableName).iterator().next());
    }

}
