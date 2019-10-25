package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Qualifier implements Serializable {

    private String classid;
    private String classname;
    private String schemeid;
    private String schemename;

//    private DataInfo dataInfo;

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

//    public DataInfo getDataInfo() {
//        return dataInfo;
//    }
//
//    public Qualifier setDataInfo(DataInfo dataInfo) {
//        this.dataInfo = dataInfo;
//        return this;
//    }
}
