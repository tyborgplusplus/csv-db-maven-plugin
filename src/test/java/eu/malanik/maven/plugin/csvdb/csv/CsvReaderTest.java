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
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/simple"), ';', new SystemStreamLog());

        Map<String, TableData> result = underTest.readData(new HashSet<>(0), new HashMap<>(0));

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("SETTING"));

        TableData data = result.get("SETTING");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("KEY"));
        Assertions.assertTrue(data.getColumnNames().contains("VALUE"));

        Assertions.assertEquals(2, data.getRows().size());
        Assertions.assertNull(data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getViews().isEmpty());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("100", data.getRows().get(0).getValuesByColumnName().get("VALUE"));

        Assertions.assertNull(data.getRows().get(1).getId());
        Assertions.assertTrue(data.getRows().get(1).getViews().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(1).getValuesByColumnName().get("KEY"));
        Assertions.assertNull(data.getRows().get(1).getValuesByColumnName().get("VALUE"));

    }

    @Test
    public void readComplexDataWithoutView() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/complex"), new SystemStreamLog());

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>();
        primaryKeyColumnsByTableName.put("SETTING", new HashSet<>(Collections.singletonList("KEY")));
        Map<String, TableData> result = underTest.readData(new HashSet<>(0), primaryKeyColumnsByTableName);

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("SETTING"));

        TableData data = result.get("SETTING");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("KEY"));
        Assertions.assertTrue(data.getColumnNames().contains("VALUE"));

        Assertions.assertEquals(3, data.getRows().size());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getViews().isEmpty());
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(0).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("100", data.getRows().get(0).getValuesByColumnName().get("VALUE"));

        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(1).getId());
        Assertions.assertTrue(data.getRows().get(1).getViews().isEmpty());
        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(1).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("http://base.malanik.eu", data.getRows().get(1).getValuesByColumnName().get("VALUE"));

        Assertions.assertEquals("VERBOSE", data.getRows().get(2).getId());
        Assertions.assertTrue(data.getRows().get(2).getViews().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(2).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("true", data.getRows().get(2).getValuesByColumnName().get("VALUE"));

    }

    @Test
    public void readComplexData() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources/complex"), new SystemStreamLog());

        Map<String, Set<String>> primaryKeyColumnsByTableName = new HashMap<>();
        primaryKeyColumnsByTableName.put("SETTING", new HashSet<>(Collections.singletonList("KEY")));
        Map<String, TableData> result = underTest.readData(new HashSet<>(Arrays.asList("US", "TEST")), primaryKeyColumnsByTableName);

        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(result.get("SETTING"));

        TableData data = result.get("SETTING");
        Assertions.assertEquals(2, data.getColumnNames().size());
        Assertions.assertTrue(data.getColumnNames().contains("KEY"));
        Assertions.assertTrue(data.getColumnNames().contains("VALUE"));

        Assertions.assertEquals(3, data.getRows().size());
        Assertions.assertEquals("VERBOSE", data.getRows().get(0).getId());
        Assertions.assertTrue(data.getRows().get(0).getViews().isEmpty());
        Assertions.assertEquals("VERBOSE", data.getRows().get(0).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("true", data.getRows().get(0).getValuesByColumnName().get("VALUE"));

        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(1).getId());
        Assertions.assertEquals(1, data.getRows().get(1).getViews().size());
        Assertions.assertTrue(data.getRows().get(1).getViews().contains("US"));
        Assertions.assertEquals("ITEM_QUANTITY", data.getRows().get(1).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("200", data.getRows().get(1).getValuesByColumnName().get("VALUE"));

        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(2).getId());
        Assertions.assertEquals(2, data.getRows().get(2).getViews().size());
        Assertions.assertTrue(data.getRows().get(2).getViews().contains("US"));
        Assertions.assertTrue(data.getRows().get(2).getViews().contains("TEST"));
        Assertions.assertEquals("PAYMENT_URL", data.getRows().get(2).getValuesByColumnName().get("KEY"));
        Assertions.assertEquals("http://test.malanik.eu", data.getRows().get(2).getValuesByColumnName().get("VALUE"));

    }


}
