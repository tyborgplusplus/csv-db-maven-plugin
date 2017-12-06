package eu.malanik.setting.csv;

import eu.malanik.setting.TableData;
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
        assertEquals(2, data.getDataByColumnName().size());
        assertEquals(2, data.getDataByColumnName().get("key").size());
        assertEquals(2, data.getDataByColumnName().get("value").size());

    }

}
