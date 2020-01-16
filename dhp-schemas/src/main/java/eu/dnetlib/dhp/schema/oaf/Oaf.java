package eu.dnetlib.dhp.schema.oaf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
