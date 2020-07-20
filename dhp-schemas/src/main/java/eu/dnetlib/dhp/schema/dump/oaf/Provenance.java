package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

public class Provenance  implements Serializable {
    private String provenance;
    private String trust;

    public String getProvenance() {
        return provenance;
    }

    public void setProvenance(String provenance) {
        this.provenance = provenance;
    }

    public String getTrust() {
        return trust;
    }

    public void setTrust(String trust) {
        this.trust = trust;
    }

    public static Provenance newInstance(String provenance, String trust){
        Provenance p = new Provenance();
        p.provenance = provenance;
        p.trust = trust;
        return p;
    }
}
