package eu.malanik.setting.db;

import eu.malanik.setting.TableData;
import org.h2.util.IOUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbWriterTest {

    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static final String DB_URL = "jdbc:h2:~/test"; //:TRACE_LEVEL_FILE=3;TRACE_LEVEL_SYSTEM_OUT=3

    private static final String DB_USER = "sa";

    private static final String DB_PASSWORD = "";

    private Connection connection = null;

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        Statement statement;
        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS SETTING (KEY VARCHAR(30), VALUE VARCHAR(500), PRIMARY KEY(KEY))");
            statement.close();
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void writeToDb() throws Exception {
        List<String> keys = Arrays.asList("SOME_STRING", "OTHER_STRING");
        List<String> values = Arrays.asList("abc", "def");

        DbWriter underTest = new DbWriter(DB_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, null);

        Map<String, TableData> data = new HashMap<>();
        TableData tableData = new TableData();
        tableData.getDataByColumnName().put("key", keys);
        tableData.getDataByColumnName().put("value", values);
        data.put("setting", tableData);

        underTest.writeToDb(data);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM SETTING");

            int resultSetSize = 0;
            while (resultSet.next()) {
                resultSetSize++;
                assertTrue(keys.contains(resultSet.getString("key")));
                assertTrue(values.contains(resultSet.getString("value")));
            }
            assertEquals(2, resultSetSize);

            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeSilently(connection);
        }

    }

}
