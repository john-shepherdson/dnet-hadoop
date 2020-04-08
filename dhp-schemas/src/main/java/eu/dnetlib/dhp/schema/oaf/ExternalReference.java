package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExternalReference that = (ExternalReference) o;
        return Objects.equals(sitename, that.sitename) &&
                Objects.equals(label, that.label) &&
                Objects.equals(url, that.url) &&
                Objects.equals(description, that.description) &&
                Objects.equals(qualifier, that.qualifier) &&
                Objects.equals(refidentifier, that.refidentifier) &&
                Objects.equals(query, that.query) &&
                Objects.equals(dataInfo, that.dataInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sitename, label, url, description, qualifier, refidentifier, query, dataInfo);
    }
}
