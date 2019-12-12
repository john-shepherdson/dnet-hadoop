package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public abstract class Oaf implements Serializable {
    
    private DataInfo dataInfo;

    private Long lastupdatetimestamp;

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }

    public Long getLastupdatetimestamp() {
        return lastupdatetimestamp;
    }

    public void setLastupdatetimestamp(Long lastupdatetimestamp) {
        this.lastupdatetimestamp = lastupdatetimestamp;
    }


    public void mergeOAFDataInfo(Oaf e) {
        if (e.getDataInfo()!= null && compareTrust(this,e)<0)
            dataInfo = e.getDataInfo();
    }

    protected String extractTrust(Oaf e) {
        if (e == null || e.getDataInfo()== null || e.getDataInfo().getTrust()== null)
            return  "0.0";
        return e.getDataInfo().getTrust();



    }

    protected int compareTrust(Oaf a, Oaf b) {
        return extractTrust(a).compareTo(extractTrust(b));

    }
}
