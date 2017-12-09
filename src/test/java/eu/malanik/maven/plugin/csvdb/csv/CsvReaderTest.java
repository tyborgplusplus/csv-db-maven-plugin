package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.TableData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class CsvReaderTest {

    @Test
    public void readData() throws IOException {
        CsvReader underTest = new CsvReader(Paths.get("src/test/resources").toFile());

        Map<String, TableData> result = underTest.readData();

        assertEquals(1, result.size());
        assertNotNull(result.get("t_setting"));

        TableData data = result.get("t_setting");
        assertEquals(2, data.getRowValuesByColumnName().size());
        assertEquals(2, data.getRowValuesByColumnName().get("key").size());
        assertEquals(2, data.getRowValuesByColumnName().get("value").size());

    }

}
