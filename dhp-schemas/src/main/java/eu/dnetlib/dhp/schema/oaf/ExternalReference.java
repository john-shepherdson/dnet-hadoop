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

    public ExternalReference setSitename(String sitename) {
        this.sitename = sitename;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public ExternalReference setLabel(String label) {
        this.label = label;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ExternalReference setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalReference setDescription(String description) {
        this.description = description;
        return this;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public ExternalReference setQualifier(Qualifier qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public String getRefidentifier() {
        return refidentifier;
    }

    public ExternalReference setRefidentifier(String refidentifier) {
        this.refidentifier = refidentifier;
        return this;
    }

    public String getQuery() {
        return query;
    }

    public ExternalReference setQuery(String query) {
        this.query = query;
        return this;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public ExternalReference setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
