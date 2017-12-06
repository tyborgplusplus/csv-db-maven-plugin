package eu.malanik.setting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableData {

    private Map<String, List<String>> dataByColumnName = new HashMap();

    public Map<String, List<String>> getDataByColumnName() {
        return dataByColumnName;
    }

}
