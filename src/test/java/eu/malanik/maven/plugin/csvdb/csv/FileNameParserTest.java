package eu.malanik.maven.plugin.csvdb.csv;

import eu.malanik.maven.plugin.csvdb.FileNameData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileNameParserTest {

    @Test
    public void extractSimpleTableName() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("SETTING.CSV");

        Assertions.assertEquals("SETTING", fileNameData.getTableName());
        Assertions.assertEquals(0, fileNameData.getViews().size());

    }

    @Test
    public void extract2WordsTableName() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("MARKET_CONFIG.CSV");

        Assertions.assertEquals("MARKET_CONFIG", fileNameData.getTableName());
        Assertions.assertEquals(0, fileNameData.getViews().size());

    }

    @Test
    public void extractSimpleTableNameWithMarks() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("SETTING__DE.CSV");

        Assertions.assertEquals("SETTING", fileNameData.getTableName());
        Assertions.assertEquals(1, fileNameData.getViews().size());
        Assertions.assertEquals("DE", fileNameData.getViews().toArray()[0]);

    }

    @Test
    public void extract2WordsTableNameWithMarks() {

        FileNameData fileNameData = FileNameParser.parseCsvFileName("MARKET_CONFIG__DE_TEST.CSV");

        Assertions.assertEquals("MARKET_CONFIG", fileNameData.getTableName());
        Assertions.assertEquals(2, fileNameData.getViews().size());
        Assertions.assertEquals("DE", fileNameData.getViews().toArray()[0]);
        Assertions.assertEquals("TEST", fileNameData.getViews().toArray()[1]);

    }



}
