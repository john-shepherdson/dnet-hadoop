package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Instance implements Serializable {

    private Field<String> license;

    private Qualifier accessright;

    private Qualifier instancetype;

    private KeyValue hostedby;

    private List<String> url;

    // other research products specifc
    private  String distributionlocation;

    private KeyValue collectedfrom;

    private Field<String> dateofacceptance;

    // ( article | book ) processing charges. Defined here to cope with possible wrongly typed results
    private Field<String> processingchargeamount;

    // currency - alphabetic code describe in ISO-4217. Defined here to cope with possible wrongly typed results
    private Field<String> processingchargecurrency;

    private Field<String> refereed; //peer-review status

    public Field<String> getLicense() {
        return license;
    }

    public void setLicense(Field<String> license) {
        this.license = license;
    }

    public Qualifier getAccessright() {
        return accessright;
    }

    public void setAccessright(Qualifier accessright) {
        this.accessright = accessright;
    }

    public Qualifier getInstancetype() {
        return instancetype;
    }

    public void setInstancetype(Qualifier instancetype) {
        this.instancetype = instancetype;
    }

    public KeyValue getHostedby() {
        return hostedby;
    }

    public void setHostedby(KeyValue hostedby) {
        this.hostedby = hostedby;
    }

    public List<String> getUrl() {
        return url;
    }

    public void setUrl(List<String> url) {
        this.url = url;
    }

    public String getDistributionlocation() {
        return distributionlocation;
    }

    public void setDistributionlocation(String distributionlocation) {
        this.distributionlocation = distributionlocation;
    }

    public KeyValue getCollectedfrom() {
        return collectedfrom;
    }

    public void setCollectedfrom(KeyValue collectedfrom) {
        this.collectedfrom = collectedfrom;
    }

    public Field<String> getDateofacceptance() {
        return dateofacceptance;
    }

    public void setDateofacceptance(Field<String> dateofacceptance) {
        this.dateofacceptance = dateofacceptance;
    }

    public Field<String> getProcessingchargeamount() {
        return processingchargeamount;
    }

    public void setProcessingchargeamount(Field<String> processingchargeamount) {
        this.processingchargeamount = processingchargeamount;
    }

    public Field<String> getProcessingchargecurrency() {
        return processingchargecurrency;
    }

    public void setProcessingchargecurrency(Field<String> processingchargecurrency) {
        this.processingchargecurrency = processingchargecurrency;
    }

    public Field<String> getRefereed() {
        return refereed;
    }

    public void setRefereed(Field<String> refereed) {
        this.refereed = refereed;
    }

    public String toComparableString(){
        return String.format("%s::%s::%s::%s",
                hostedby != null && hostedby.getKey()!= null  ? hostedby.getKey().toLowerCase() : "",
                accessright!= null && accessright.getClassid()!= null ? accessright.getClassid() : "",
                instancetype!= null && instancetype.getClassid()!= null ? instancetype.getClassid() : "",
                url != null ? url:"");
    }

    @Override
    public int hashCode() {
        return toComparableString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Instance other = (Instance) obj;

        return toComparableString()
                .equals(other.toComparableString());
    }
}
