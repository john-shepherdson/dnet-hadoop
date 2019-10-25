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

    public Dataset setStoragedate(Field<String> storagedate) {
        this.storagedate = storagedate;
        return this;
    }

    public Field<String> getDevice() {
        return device;
    }

    public Dataset setDevice(Field<String> device) {
        this.device = device;
        return this;
    }

    public Field<String> getSize() {
        return size;
    }

    public Dataset setSize(Field<String> size) {
        this.size = size;
        return this;
    }

    public Field<String> getVersion() {
        return version;
    }

    public Dataset setVersion(Field<String> version) {
        this.version = version;
        return this;
    }

    public Field<String> getLastmetadataupdate() {
        return lastmetadataupdate;
    }

    public Dataset setLastmetadataupdate(Field<String> lastmetadataupdate) {
        this.lastmetadataupdate = lastmetadataupdate;
        return this;
    }

    public Field<String> getMetadataversionnumber() {
        return metadataversionnumber;
    }

    public Dataset setMetadataversionnumber(Field<String> metadataversionnumber) {
        this.metadataversionnumber = metadataversionnumber;
        return this;
    }

    public List<GeoLocation> getGeolocation() {
        return geolocation;
    }

    public Dataset setGeolocation(List<GeoLocation> geolocation) {
        this.geolocation = geolocation;
        return this;
    }
}
