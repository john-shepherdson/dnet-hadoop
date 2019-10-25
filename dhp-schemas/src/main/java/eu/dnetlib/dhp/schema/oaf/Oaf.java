package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public abstract class Oaf implements Serializable {
    
    private DataInfo dataInfo;

    private Long lastupdatetimestamp;

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public Oaf setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }

    public Long getLastupdatetimestamp() {
        return lastupdatetimestamp;
    }

    public Oaf setLastupdatetimestamp(Long lastupdatetimestamp) {
        this.lastupdatetimestamp = lastupdatetimestamp;
        return this;
    }
}
