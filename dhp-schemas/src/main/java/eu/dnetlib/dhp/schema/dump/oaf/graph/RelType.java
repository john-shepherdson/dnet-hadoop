package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class RelType implements Serializable {
    private String name ; //relclass
    private String type ; //subreltype

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
