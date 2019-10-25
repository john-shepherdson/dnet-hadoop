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

    public Field<String> getStoragedate() {
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
}
