package eu.dnetlib.dhp.model.mdstore;

import eu.dnetlib.dhp.utils.DHPUtils;

import java.io.Serializable;


/**
 * This class models a record inside the new Metadata store collection on HDFS *
 *
 */
public class MetadataRecord implements Serializable {

    /**
     * The D-Net Identifier associated to the record
     */
    private String id;

    /**
     * The original Identifier of the record
     */
    private String originalId;


    /**
     * The encoding of the record, should be JSON or XML
     */
    private String encoding;

    /**
     * The information about the provenance of the record see @{@link Provenance}
     * for the model of this information
     */
    private Provenance provenance;

    /**
     * The content of the metadata
     */
    private String body;

    /**
     * the date when the record has been stored
     */
    private long dateOfCollection;

    /**
     * the date when the record has been stored
     */
    private long dateOfTransformation;


    public MetadataRecord() {
        this.dateOfCollection = System.currentTimeMillis();
    }

    public MetadataRecord(String originalId, String encoding, Provenance provenance, String body, long dateOfCollection) {

        this.originalId = originalId;
        this.encoding = encoding;
        this.provenance = provenance;
        this.body = body;
        this.dateOfCollection = dateOfCollection;
        this.id = DHPUtils.generateIdentifier(originalId,this.provenance.getNsPrefix());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getOriginalId() {
        return originalId;
    }

    public void setOriginalId(String originalId) {
        this.originalId = originalId;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Provenance getProvenance() {
        return provenance;
    }

    public void setProvenance(Provenance provenance) {
        this.provenance = provenance;
    }


    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public long getDateOfCollection() {
        return dateOfCollection;
    }

    public void setDateOfCollection(long dateOfCollection) {
        this.dateOfCollection = dateOfCollection;
    }

    public long getDateOfTransformation() {
        return dateOfTransformation;
    }

    public void setDateOfTransformation(long dateOfTransformation) {
        this.dateOfTransformation = dateOfTransformation;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetadataRecord)) {
            return false;
        }
        return ((MetadataRecord) o).getId().equalsIgnoreCase(id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
