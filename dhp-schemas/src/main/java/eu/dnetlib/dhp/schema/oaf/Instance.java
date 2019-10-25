package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Instance implements Serializable {

    private Field<String> license;

    private Qualifier accessright;

    private Qualifier instancetype;

    private KeyValue hostedby;

    private String url;

    // other research products specifc
    private  String distributionlocation;

    private KeyValue collectedfrom;

    private Field<String> dateofacceptance;

    public Field<String> getLicense() {
        return license;
    }

    public Instance setLicense(Field<String> license) {
        this.license = license;
        return this;
    }

    public Qualifier getAccessright() {
        return accessright;
    }

    public Instance setAccessright(Qualifier accessright) {
        this.accessright = accessright;
        return this;
    }

    public Qualifier getInstancetype() {
        return instancetype;
    }

    public Instance setInstancetype(Qualifier instancetype) {
        this.instancetype = instancetype;
        return this;
    }

    public KeyValue getHostedby() {
        return hostedby;
    }

    public Instance setHostedby(KeyValue hostedby) {
        this.hostedby = hostedby;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public Instance setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getDistributionlocation() {
        return distributionlocation;
    }

    public Instance setDistributionlocation(String distributionlocation) {
        this.distributionlocation = distributionlocation;
        return this;
    }

    public KeyValue getCollectedfrom() {
        return collectedfrom;
    }

    public Instance setCollectedfrom(KeyValue collectedfrom) {
        this.collectedfrom = collectedfrom;
        return this;
    }

    public Field<String> getDateofacceptance() {
        return dateofacceptance;
    }

    public Instance setDateofacceptance(Field<String> dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
        return this;
    }
}
