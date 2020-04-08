package eu.dnetlib.dhp.oa.provision.model;

import com.google.common.base.Objects;

import java.io.Serializable;

public class TypedRow implements Serializable {

    private String id;

    private Boolean deleted;

    private String type;

    private String oaf;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOaf() {
        return oaf;
    }

    public void setOaf(String oaf) {
        this.oaf = oaf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypedRow typedRow2 = (TypedRow) o;
        return Objects.equal(id, typedRow2.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
