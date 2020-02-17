package eu.dnetlib.dhp.graph.utils;

import java.io.Serializable;

public class ContextDef implements Serializable {

    private String id;
    private String label;
    private String name;
    private String type;

    public ContextDef(final String id, final String label, final String name, final String type) {
        super();
        this.setId(id);
        this.setLabel(label);
        this.setName(name);
        this.setType(type);
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(final String label) {
        this.label = label;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }
}