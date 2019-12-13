package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Context implements Serializable {
    private String id;

    private List<DataInfo> dataInfo;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<DataInfo> getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(List<DataInfo> dataInfo) {
        this.dataInfo = dataInfo;
    }

    @Override
    public int hashCode() {
        return id ==null? 0 : id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Context other = (Context) obj;

        return id.equals(other.getId());
    }
}
