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

    public void setClassid(String classid) {
        this.classid = classid;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public String getSchemeid() {
        return schemeid;
    }

    public void setSchemeid(String schemeid) {
        this.schemeid = schemeid;
    }

    public String getSchemename() {
        return schemename;
    }

    public void setSchemename(String schemename) {
        this.schemename = schemename;
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
