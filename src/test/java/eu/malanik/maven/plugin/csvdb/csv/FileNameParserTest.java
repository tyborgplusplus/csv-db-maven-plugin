package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.FileNameData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileNameParserTest {

    @Test
    public void extractSimpleTableName() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("setting.csv");

        Assertions.assertEquals("setting", fileNameData.getTableName());
        Assertions.assertEquals(0, fileNameData.getFilters().size());

    }

    @Test
    public void extract2WordsTableName() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("market_config.csv");

        Assertions.assertEquals("market_config", fileNameData.getTableName());
        Assertions.assertEquals(0, fileNameData.getFilters().size());

    }

    @Test
    public void extractSimpleTableNameWithMarks() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("setting__de.csv");

        Assertions.assertEquals("setting", fileNameData.getTableName());
        Assertions.assertEquals(1, fileNameData.getFilters().size());
        Assertions.assertEquals("de", fileNameData.getFilters().toArray()[0]);

    }

    @Test
    public void extract2WordsTableNameWithMarks() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("market_config__de_test.csv");

        Assertions.assertEquals("market_config", fileNameData.getTableName());
        Assertions.assertEquals(2, fileNameData.getFilters().size());
        Assertions.assertEquals("de", fileNameData.getFilters().toArray()[0]);
        Assertions.assertEquals("test", fileNameData.getFilters().toArray()[1]);

    }



}
