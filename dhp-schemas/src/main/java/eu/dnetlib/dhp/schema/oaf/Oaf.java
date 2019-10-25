package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public abstract class Oaf<T extends Oaf<T>> implements Serializable {
    
    private DataInfo dataInfo;

    private Long lastupdatetimestamp;

    protected abstract T self();

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public T setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return self();
    }

    public Long getLastupdatetimestamp() {
        return lastupdatetimestamp;
    }

    public T setLastupdatetimestamp(Long lastupdatetimestamp) {
        this.lastupdatetimestamp = lastupdatetimestamp;
        return self();
    }
}
