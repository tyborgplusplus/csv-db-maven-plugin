package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.TableData;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CsvReaderTest {

    @Test
    public void readSimpleData() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/simple"), new SystemStreamLog());

        Map<String, TableData> result = underTest.readData(new HashSet<>(0), new HashMap<>(0));

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("setting"));

        TableData data = result.get("setting");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("key"));
        Assertions.assertTrue(data.getColumnNames().contains("value"));

        Assertions.assertEquals(2, data.getRows().size());
        Assertions.assertNull(data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getFilters().isEmpty());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getValuesByColumnName().get("key"));
        Assertions.assertEquals("100", data.getRows().get(0).getValuesByColumnName().get("value"));

        Assertions.assertNull(data.getRows().get(1).getId());
        Assertions.assertTrue(data.getRows().get(1).getFilters().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(1).getValuesByColumnName().get("key"));
        Assertions.assertNull(data.getRows().get(1).getValuesByColumnName().get("value"));

    }

    @Test
    public void readComplexDataWithoutFilter() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/complex"), new SystemStreamLog());

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>();
        primaryKeyColumnsByTableName.put("setting", new HashSet<>(Collections.singletonList("key")));
        Map<String, TableData> result = underTest.readData(new HashSet<>(0), primaryKeyColumnsByTableName);

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("setting"));

        TableData data = result.get("setting");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("key"));
        Assertions.assertTrue(data.getColumnNames().contains("value"));

        Assertions.assertEquals(3, data.getRows().size());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getFilters().isEmpty());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getValuesByColumnName().get("key"));
        Assertions.assertEquals("100", data.getRows().get(0).getValuesByColumnName().get("value"));

        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(1).getId());
        Assertions.assertTrue(data.getRows().get(1).getFilters().isEmpty());
        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(1).getValuesByColumnName().get("key"));
        Assertions.assertEquals("http://base.malanik.eu", data.getRows().get(1).getValuesByColumnName().get("value"));

        Assertions.assertEquals("VERBOSE", data.getRows().get(2).getId());
        Assertions.assertTrue(data.getRows().get(2).getFilters().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(2).getValuesByColumnName().get("key"));
        Assertions.assertEquals("true", data.getRows().get(2).getValuesByColumnName().get("value"));

    }

    @Test
    public void readComplexData() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/complex"), new SystemStreamLog());

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>();
        primaryKeyColumnsByTableName.put("setting", new HashSet<>(Collections.singletonList("key")));
        Map<String, TableData> result = underTest.readData(new HashSet<>(Arrays.asList("us", "test")), primaryKeyColumnsByTableName);

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("setting"));

        TableData data = result.get("setting");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("key"));
        Assertions.assertTrue(data.getColumnNames().contains("value"));

        Assertions.assertEquals(3, data.getRows().size());
        Assertions.assertEquals("VERBOSE", data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getFilters().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(0).getValuesByColumnName().get("key"));
        Assertions.assertEquals("true", data.getRows().get(0).getValuesByColumnName().get("value"));

        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(1).getId());
        Assertions.assertEquals(1, data.getRows().get(1).getFilters().size());
        Assertions.assertTrue(data.getRows().get(1).getFilters().contains("us"));
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(1).getValuesByColumnName().get("key"));
        Assertions.assertEquals("200", data.getRows().get(1).getValuesByColumnName().get("value"));

        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(2).getId());
        Assertions.assertEquals(2, data.getRows().get(2).getFilters().size());
        Assertions.assertTrue(data.getRows().get(2).getFilters().contains("us"));
        Assertions.assertTrue(data.getRows().get(2).getFilters().contains("test"));
        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(2).getValuesByColumnName().get("key"));
        Assertions.assertEquals("http://test.malanik.eu", data.getRows().get(2).getValuesByColumnName().get("value"));

    }

}
