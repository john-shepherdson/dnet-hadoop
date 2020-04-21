package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public abstract class OafEntity extends Oaf implements Serializable {

    private String id;

    private List<String> originalId;

    private List<StructuredProperty> pid;

    private String dateofcollection;

    private String dateoftransformation;

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

    public void mergeFrom(OafEntity e) {

        if (e == null) return;

        originalId = mergeLists(originalId, e.getOriginalId());

        collectedfrom = mergeLists(collectedfrom, e.getCollectedfrom());

        pid = mergeLists(pid, e.getPid());

        if (e.getDateofcollection() != null && compareTrust(this, e) < 0)
            dateofcollection = e.getDateofcollection();

        if (e.getDateoftransformation() != null && compareTrust(this, e) < 0)
            dateoftransformation = e.getDateoftransformation();

        extraInfo = mergeLists(extraInfo, e.getExtraInfo());

        if (e.getOaiprovenance() != null && compareTrust(this, e) < 0)
            oaiprovenance = e.getOaiprovenance();
    }

    protected <T> List<T> mergeLists(final List<T>... lists) {

        return Arrays.stream(lists)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        OafEntity oafEntity = (OafEntity) o;
        return Objects.equals(id, oafEntity.id)
                && Objects.equals(originalId, oafEntity.originalId)
                && Objects.equals(collectedfrom, oafEntity.collectedfrom)
                && Objects.equals(pid, oafEntity.pid)
                && Objects.equals(dateofcollection, oafEntity.dateofcollection)
                && Objects.equals(dateoftransformation, oafEntity.dateoftransformation)
                && Objects.equals(extraInfo, oafEntity.extraInfo)
                && Objects.equals(oaiprovenance, oafEntity.oaiprovenance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                id,
                originalId,
                collectedfrom,
                pid,
                dateofcollection,
                dateoftransformation,
                extraInfo,
                oaiprovenance);
    }
}
