package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class ExternalReference implements Serializable {
    // source
    private String sitename;

    // title
    private String label;

    // text()
    private String url;

    // ?? not mapped yet ??
    private String description;

    // type
    private Qualifier qualifier;

    // site internal identifier
    private String refidentifier;

    // maps the oaf:reference/@query attribute
    private String query;

    // ExternalReferences might be also inferred
    private DataInfo dataInfo;

    public String getSitename() {
        return sitename;
    }

    public void setSitename(String sitename) {
        this.sitename = sitename;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public void setQualifier(Qualifier qualifier) {
        this.qualifier = qualifier;
    }

    public String getRefidentifier() {
        return refidentifier;
    }

    public void setRefidentifier(String refidentifier) {
        this.refidentifier = refidentifier;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }
}
