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

    private List<ExtraInfo> extraInfo;

    private OAIProvenance oaiprovenance;


    public String getId() {
        return id;
    }

    public OafEntity setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getOriginalId() {
        return originalId;
    }

    public OafEntity setOriginalId(List<String> originalId) {
        this.originalId = originalId;
        return this;
    }

    public List<KeyValue> getCollectedfrom() {
        return collectedfrom;
    }

    public OafEntity setCollectedfrom(List<KeyValue> collectedfrom) {
        this.collectedfrom = collectedfrom;
        return this;
    }

    public List<StructuredProperty> getPid() {
        return pid;
    }

    public OafEntity setPid(List<StructuredProperty> pid) {
        this.pid = pid;
        return this;
    }

    public String getDateofcollection() {
        return dateofcollection;
    }

    public OafEntity setDateofcollection(String dateofcollection) {
        this.dateofcollection = dateofcollection;
        return this;
    }

    public String getDateoftransformation() {
        return dateoftransformation;
    }

    public OafEntity setDateoftransformation(String dateoftransformation) {
        this.dateoftransformation = dateoftransformation;
        return this;
    }

    public List<ExtraInfo> getExtraInfo() {
        return extraInfo;
    }

    public OafEntity setExtraInfo(List<ExtraInfo> extraInfo) {
        this.extraInfo = extraInfo;
        return this;
    }

    public OAIProvenance getOaiprovenance() {
        return oaiprovenance;
    }

    public OafEntity setOaiprovenance(OAIProvenance oaiprovenance) {
        this.oaiprovenance = oaiprovenance;
        return this;
    }
}
