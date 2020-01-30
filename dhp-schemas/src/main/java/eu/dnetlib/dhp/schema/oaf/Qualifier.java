package eu.dnetlib.dhp.schema.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class Qualifier implements Serializable {

    private String classid;
    private String classname;
    private String schemeid;
    private String schemename;

    public String getClassid() {
        return classid;
    }

    public Qualifier setClassid(String classid) {
        this.classid = classid;
        return this;
    }

    public String getClassname() {
        return classname;
    }

    public Qualifier setClassname(String classname) {
        this.classname = classname;
        return this;
    }

    public String getSchemeid() {
        return schemeid;
    }

    public Qualifier setSchemeid(String schemeid) {
        this.schemeid = schemeid;
        return this;
    }

    public String getSchemename() {
        return schemename;
    }

    public Qualifier setSchemename(String schemename) {
        this.schemename = schemename;
        return this;
    }

    public String toComparableString() {
        return isBlank()?"": String.format("%s::%s::%s::%s",
                classid != null ? classid : "",
                classname != null ? classname : "",
                schemeid != null ? schemeid : "",
                schemename != null ? schemename : "");
    }

    @JsonIgnore
    public boolean isBlank() {
        return StringUtils.isBlank(classid) &&
                StringUtils.isBlank(classname) &&
                StringUtils.isBlank(schemeid) &&
                StringUtils.isBlank(schemename);
    }
    @Override
    public int hashCode() {
        return toComparableString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Qualifier other = (Qualifier) obj;

        return toComparableString()
                .equals(other.toComparableString());
    }
}
