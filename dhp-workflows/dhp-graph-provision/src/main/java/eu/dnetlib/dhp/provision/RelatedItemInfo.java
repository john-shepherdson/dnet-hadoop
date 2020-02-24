package eu.dnetlib.dhp.provision;

import java.io.Serializable;

/**
 * This class models the information of related items
 */

public class RelatedItemInfo implements Serializable {

    private String id;

    private int relatedDataset = 0;

    private int relatedPublication = 0;

    private int relatedUnknown = 0;


    public String getId() {
        return id;
    }

    public RelatedItemInfo setId(String id) {
        this.id = id;
        return this;
    }

    public RelatedItemInfo add(RelatedItemInfo other) {
        if (other != null) {
            relatedDataset += other.getRelatedDataset();
            relatedPublication += other.getRelatedPublication();
            relatedUnknown += other.getRelatedUnknown();
        }
        return this;
    }

    public int getRelatedDataset() {
        return relatedDataset;
    }

    public RelatedItemInfo setRelatedDataset(int relatedDataset) {
        this.relatedDataset = relatedDataset;
        return this;
    }

    public int getRelatedPublication() {
        return relatedPublication;
    }

    public RelatedItemInfo setRelatedPublication(int relatedPublication) {
        this.relatedPublication = relatedPublication;
        return this;
    }

    public int getRelatedUnknown() {
        return relatedUnknown;
    }

    public RelatedItemInfo setRelatedUnknown(int relatedUnknown) {
        this.relatedUnknown = relatedUnknown;
        return this;
    }
}
