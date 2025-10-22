package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;

public class SelectPair implements Serializable {
    private String tableName;
    private String tableSelect;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableSelect() {
        return tableSelect;
    }

    public void setTableSelect(String tableSelect) {
        this.tableSelect = tableSelect;
    }
}
