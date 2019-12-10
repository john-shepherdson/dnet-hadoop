package eu.dnetlib.dedup;

import java.io.Serializable;
public class OafKey implements Serializable {

    private String dedupId;
    private String trust;

    public OafKey(String dedupId, String trust) {
        this.dedupId = dedupId;
        this.trust = trust;
    }
    public OafKey() {
    }
    public String getDedupId() {
        return dedupId;
    }
    public void setDedupId(String dedupId) {
        this.dedupId = dedupId;
    }
    public String getTrust() {
        return trust;
    }
    public void setTrust(String trust) {
        this.trust = trust;
    }
    @Override
    public String toString(){
        return String.format("%s->%d", dedupId,trust);
    }
}
