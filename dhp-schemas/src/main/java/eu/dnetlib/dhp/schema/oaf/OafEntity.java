package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public abstract class OafEntity<T extends OafEntity<T>> extends Oaf<T> implements Serializable {

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

    public T setId(String id) {
        this.id = id;
        return self();
    }

    public List<String> getOriginalId() {
        return originalId;
    }

    public T setOriginalId(List<String> originalId) {
        this.originalId = originalId;
        return self();
    }

    public List<KeyValue> getCollectedfrom() {
        return collectedfrom;
    }

    public T setCollectedfrom(List<KeyValue> collectedfrom) {
        this.collectedfrom = collectedfrom;
        return self();
    }

    public List<StructuredProperty> getPid() {
        return pid;
    }

    public T setPid(List<StructuredProperty> pid) {
        this.pid = pid;
        return self();
    }

    public String getDateofcollection() {
        return dateofcollection;
    }

    public T setDateofcollection(String dateofcollection) {
        this.dateofcollection = dateofcollection;
        return self();
    }

    public String getDateoftransformation() {
        return dateoftransformation;
    }

    public T setDateoftransformation(String dateoftransformation) {
        this.dateoftransformation = dateoftransformation;
        return self();
    }

    public List<ExtraInfo> getExtraInfo() {
        return extraInfo;
    }

    public T setExtraInfo(List<ExtraInfo> extraInfo) {
        this.extraInfo = extraInfo;
        return self();
    }

    public OAIProvenance getOaiprovenance() {
        return oaiprovenance;
    }

    public T setOaiprovenance(OAIProvenance oaiprovenance) {
        this.oaiprovenance = oaiprovenance;
        return self();
    }
}
