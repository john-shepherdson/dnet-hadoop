package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;


import java.io.Serializable;
import java.util.List;

public class SerializationOrg implements Serializable {
    List<SerializationMatching> matchings;

    private String raw;

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public List<SerializationMatching> getMatchings() {
        return matchings;
    }

    public void setMatchings(List<SerializationMatching> matchings) {
        this.matchings = matchings;
    }
}
