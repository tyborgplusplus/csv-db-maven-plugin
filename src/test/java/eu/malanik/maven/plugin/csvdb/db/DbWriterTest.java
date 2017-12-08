package eu.malanik.maven.plugin.csvdb.db;

import eu.malanik.maven.plugin.csvdb.TableData;
import org.h2.util.IOUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbWriterTest {

    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static final String DB_URL = "jdbc:h2:./target/DbWriterTest;TRACE_LEVEL_SYSTEM_OUT=2";

    private static final String DB_USER = "sa";

    private static final String DB_PASSWORD = "";

    private static final String DATE_FORMAT = "dd.MM.yyyy";

    private static final String TIMESTAMP_FORMAT = "dd.MM.yyyy HH:mm:ss";

    private Connection connection = null;

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        Statement statement;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS SETTING ("
                    + "STRING VARCHAR(30),"
                    + "INTEGER INT,"
                    + "DOUBLE DOUBLE,"
                    + "BOOLEAN BOOLEAN,"
                    + "DATE DATE,"
                    + "TIMESTAMP TIMESTAMP)");
            statement.close();
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void writeToDb() throws Exception {
        List<String> strings = Arrays.asList("text", null);
        List<String> integers = Arrays.asList("5", null);
        List<String> doubles = Arrays.asList("3.14", null);
        List<String> booleans = Arrays.asList("TRUE", null);
        List<String> dates = Arrays.asList("now", null);
        List<String> timestamps = Arrays.asList("01.07.2009 19:23:55", null);

        DbWriter underTest = new DbWriter(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        tableData.getDataByColumnName().put("string", strings);
        tableData.getDataByColumnName().put("integer", integers);
        tableData.getDataByColumnName().put("double", doubles);
        tableData.getDataByColumnName().put("boolean", booleans);
        tableData.getDataByColumnName().put("date", dates);
        tableData.getDataByColumnName().put("timestamp", timestamps);
        data.put("setting", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
                assertEquals("text", resultSet.getString("string"));

                assertEquals(5, resultSet.getInt("integer"));

                assertEquals(BigDecimal.valueOf(3.14), BigDecimal.valueOf(resultSet.getDouble("double")));

                assertEquals(Boolean.TRUE, resultSet.getBoolean("boolean"));

                assertEquals(LocalDate.now(), resultSet.getDate("date").toLocalDate());

                assertEquals(
                        LocalDateTime.parse("01.07.2009 19:23:55", DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT)),
                        resultSet.getTimestamp("timestamp").toLocalDateTime());
            }
            assertEquals(1, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }

    @Test
    public void writeToDbNoRows() throws Exception {
        DbWriter underTest = new DbWriter(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        tableData.getDataByColumnName().put("string", new ArrayList<>());
        tableData.getDataByColumnName().put("integer", new ArrayList<>());
        tableData.getDataByColumnName().put("double", new ArrayList<>());
        tableData.getDataByColumnName().put("boolean", new ArrayList<>());
        tableData.getDataByColumnName().put("date", new ArrayList<>());
        tableData.getDataByColumnName().put("timestamp", new ArrayList<>());
        data.put("setting", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
            }
            assertEquals(0, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }

    @Test
    public void writeToDbNoColumns() throws Exception {
        DbWriter underTest = new DbWriter(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        data.put("setting", tableData);

        underTest.writeToDb(data, DATE_FORMAT, TIMESTAMP_FORMAT);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
            }
            assertEquals(0, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }


}
