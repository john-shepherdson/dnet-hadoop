package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public abstract class OafEntity extends Oaf implements Serializable {

    private String id;

    private List<String> originalId;

    private List<KeyValue> collectedfrom;

    private List<StructuredProperty> pid;

    private String dateofcollection;

    private String dateoftransformation;

    //TODO remove this field
    private List<OafEntity> children;

    private List<ExtraInfo> extraInfo;

    private OAIProvenance oaiprovenance;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getOriginalId() {
        return originalId;
    }

    public void setOriginalId(List<String> originalId) {
        this.originalId = originalId;
    }

    public List<KeyValue> getCollectedfrom() {
        return collectedfrom;
    }

    public void setCollectedfrom(List<KeyValue> collectedfrom) {
        this.collectedfrom = collectedfrom;
    }

    public List<StructuredProperty> getPid() {
        return pid;
    }

    public void setPid(List<StructuredProperty> pid) {
        this.pid = pid;
    }

    public String getDateofcollection() {
        return dateofcollection;
    }

    public void setDateofcollection(String dateofcollection) {
        this.dateofcollection = dateofcollection;
    }

    public String getDateoftransformation() {
        return dateoftransformation;
    }

    public void setDateoftransformation(String dateoftransformation) {
        this.dateoftransformation = dateoftransformation;
    }

    public List<OafEntity> getChildren() {
        return children;
    }

    public void setChildren(List<OafEntity> children) {
        this.children = children;
    }

    public List<ExtraInfo> getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(List<ExtraInfo> extraInfo) {
        this.extraInfo = extraInfo;
    }

    public OAIProvenance getOaiprovenance() {
        return oaiprovenance;
    }

    public void setOaiprovenance(OAIProvenance oaiprovenance) {
        this.oaiprovenance = oaiprovenance;
    }
}
