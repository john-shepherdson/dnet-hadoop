package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class OtherResearchProducts extends Result<OtherResearchProducts> implements Serializable {

    private List<Field<String>> contactperson;

    private List<Field<String>> contactgroup;

    private List<Field<String>> tool;

    public List<Field<String>> getContactperson() {
        return contactperson;
    }

    public OtherResearchProducts setContactperson(List<Field<String>> contactperson) {
        this.contactperson = contactperson;
        return this;
    }

    public List<Field<String>> getContactgroup() {
        return contactgroup;
    }

    public OtherResearchProducts setContactgroup(List<Field<String>> contactgroup) {
        this.contactgroup = contactgroup;
        return this;
    }

    public List<Field<String>> getTool() {
        return tool;
    }

    public OtherResearchProducts setTool(List<Field<String>> tool) {
        this.tool = tool;
        return this;
    }

    @Override
    protected OtherResearchProducts self() {
        return this;
    }
}
