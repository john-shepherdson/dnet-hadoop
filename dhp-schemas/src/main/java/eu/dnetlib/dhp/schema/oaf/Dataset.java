package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Dataset extends Result implements Serializable {

    private Field<String> storagedate;

    private Field<String> device;

    private Field<String> size;

    private Field<String> version;

    private Field<String> lastmetadataupdate;

    private Field<String> metadataversionnumber;

    private List<GeoLocation> geolocation;

    public  Field<String> getStoragedate() {
        return storagedate;
    }

    public void setStoragedate(Field<String> storagedate) {
        this.storagedate = storagedate;
    }

    public Field<String> getDevice() {
        return device;
    }

    public void setDevice(Field<String> device) {
        this.device = device;
    }

    public Field<String> getSize() {
        return size;
    }

    public void setSize(Field<String> size) {
        this.size = size;
    }

    public Field<String> getVersion() {
        return version;
    }

    public void setVersion(Field<String> version) {
        this.version = version;
    }

    public Field<String> getLastmetadataupdate() {
        return lastmetadataupdate;
    }

    public void setLastmetadataupdate(Field<String> lastmetadataupdate) {
        this.lastmetadataupdate = lastmetadataupdate;
    }

    public Field<String> getMetadataversionnumber() {
        return metadataversionnumber;
    }

    public void setMetadataversionnumber(Field<String> metadataversionnumber) {
        this.metadataversionnumber = metadataversionnumber;
    }

    public List<GeoLocation> getGeolocation() {
        return geolocation;
    }

    public void setGeolocation(List<GeoLocation> geolocation) {
        this.geolocation = geolocation;
    }

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);
        final Dataset d = (Dataset) e;

        storagedate = d.getStoragedate() != null && compareTrust(this, e)<0? d.getStoragedate() : storagedate;

        device= d.getDevice() != null && compareTrust(this, e)<0? d.getDevice() : device;

        size= d.getSize() != null && compareTrust(this, e)<0? d.getSize() : size;

        version= d.getVersion() != null && compareTrust(this, e)<0? d.getVersion() : version;

        lastmetadataupdate= d.getLastmetadataupdate() != null && compareTrust(this, e)<0? d.getLastmetadataupdate() :lastmetadataupdate;

        metadataversionnumber= d.getMetadataversionnumber() != null && compareTrust(this, e)<0? d.getMetadataversionnumber() : metadataversionnumber;

        geolocation = mergeLists(geolocation, d.getGeolocation());

        mergeOAFDataInfo(d);
    }
}
