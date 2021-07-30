package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;

public class DatasourceInfo implements Serializable {
    private String id;
    private String officialname;
    private String issn;
    private String eissn;
    private String lissn;
    private Boolean openAccess;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOfficialname() {
        return officialname;
    }

    public void setOfficialname(String officialname) {
        this.officialname = officialname;
    }

    public String getIssn() {
        return issn;
    }

    public void setIssn(String issn) {
        this.issn = issn;
    }

    public String getEissn() {
        return eissn;
    }

    public void setEissn(String eissn) {
        this.eissn = eissn;
    }

    public String getLissn() {
        return lissn;
    }

    public void setLissn(String lissn) {
        this.lissn = lissn;
    }

    public Boolean getOpenAccess() {
        return openAccess;
    }

    public void setOpenAccess(Boolean openAccess) {
        this.openAccess = openAccess;
    }

    public static DatasourceInfo newInstance(String id, String officialname, String issn, String eissn, String lissn){
        DatasourceInfo di = new DatasourceInfo();
        di.id = id;
        di.officialname = officialname;
        di.issn = (issn != null) ? issn :  "" ;
        di.eissn = (eissn != null) ? eissn:"";
        di.lissn = (lissn != null) ? lissn : "";
        di.openAccess = false;

        return di;
    }
}
