package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public abstract class Oaf implements Serializable {
    
    private DataInfo dataInfo;

    private Long lastupdatetimestamp;


//    protected abstract <T extends Oaf> T fromJson(final String json);

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
}
