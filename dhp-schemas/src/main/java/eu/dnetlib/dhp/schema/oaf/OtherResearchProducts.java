package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class OtherResearchProducts extends Result implements Serializable {

    private List<Field<String>> contactperson;

    private List<Field<String>> contactgroup;

    private List<Field<String>> tool;

    public List<Field<String>> getContactperson() {
        return contactperson;
    }

    public void setContactperson(List<Field<String>> contactperson) {
        this.contactperson = contactperson;
    }

    public List<Field<String>> getContactgroup() {
        return contactgroup;
    }

    public void setContactgroup(List<Field<String>> contactgroup) {
        this.contactgroup = contactgroup;
    }

    public List<Field<String>> getTool() {
        return tool;
    }

    public void setTool(List<Field<String>> tool) {
        this.tool = tool;
    }
}
