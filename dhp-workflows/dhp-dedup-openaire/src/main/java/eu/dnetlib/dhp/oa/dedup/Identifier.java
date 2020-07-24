package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class Identifier implements Serializable, Comparable<Identifier>{

    StructuredProperty pid;
    Date date;
    PidType type;
    List<KeyValue> collectedFrom;
    EntityType entityType;
    String originalID;

    boolean useOriginal = false;  //to know if the top identifier won because of the alphabetical order of the original ID

    public Identifier(StructuredProperty pid, Date date, PidType type, List<KeyValue> collectedFrom, EntityType entityType, String originalID) {
        this.pid = pid;
        this.date = date;
        this.type = type;
        this.collectedFrom = collectedFrom;
        this.entityType = entityType;
        this.originalID = originalID;
    }

    public StructuredProperty getPid() {
        return pid;
    }

    public void setPid(StructuredProperty pidValue) {
        this.pid = pid;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public PidType getType() {
        return type;
    }

    public void setType(PidType type) {
        this.type = type;
    }

    public List<KeyValue> getCollectedFrom() {
        return collectedFrom;
    }

    public void setCollectedFrom(List<KeyValue> collectedFrom) {
        this.collectedFrom = collectedFrom;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public String getOriginalID() {
        return originalID;
    }

    public void setOriginalID(String originalID) {
        this.originalID = originalID;
    }

    public boolean isUseOriginal() {
        return useOriginal;
    }

    public void setUseOriginal(boolean useOriginal) {
        this.useOriginal = useOriginal;
    }

    @Override
    public int compareTo(Identifier i) {
        //priority in comparisons: 1) pidtype, 2) collectedfrom (depending on the entity type) , 3) date 4) alphabetical order of the originalID
        if (this.getType().compareTo(i.getType()) == 0){ //same type
            if (entityType == EntityType.publication) {
                if (isFromDatasourceID(this.collectedFrom, IdGenerator.CROSSREF_ID) && !isFromDatasourceID(i.collectedFrom, IdGenerator.CROSSREF_ID))
                    return 1;
                if (isFromDatasourceID(i.collectedFrom, IdGenerator.CROSSREF_ID) && !isFromDatasourceID(this.collectedFrom, IdGenerator.CROSSREF_ID))
                    return -1;
            }
            if (entityType == EntityType.dataset) {
                if (isFromDatasourceID(this.collectedFrom, IdGenerator.DATACITE_ID) && !isFromDatasourceID(i.collectedFrom, IdGenerator.DATACITE_ID))
                    return 1;
                if (isFromDatasourceID(i.collectedFrom, IdGenerator.DATACITE_ID) && !isFromDatasourceID(this.collectedFrom, IdGenerator.DATACITE_ID))
                    return -1;
            }

            if (this.getDate().compareTo(date) == 0) {//same date

                if (this.originalID.compareTo(i.originalID) > 0)
                    this.useOriginal = true;
                else
                    i.setUseOriginal(true);

                //the minus because we need to take the alphabetically lower id
                return -this.originalID.compareTo(i.originalID);
            }
            else
                //the minus is because we need to take the elder date
                return -this.getDate().compareTo(date);
        }
        else {
            return this.getType().compareTo(i.getType());
        }

    }

    public boolean isFromDatasourceID(List<KeyValue> collectedFrom, String dsId){

        for(KeyValue cf: collectedFrom) {
            if(cf.getKey().equals(dsId))
                return true;
        }
        return false;
    }
}
