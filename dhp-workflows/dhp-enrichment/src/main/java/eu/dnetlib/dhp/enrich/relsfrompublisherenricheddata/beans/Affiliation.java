package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans;


import java.io.Serializable;
import java.util.List;

public class Affiliation implements Serializable {
    private String raw_affiliation_string;
    private List<Matching> matchings;


    public String getRaw_affiliation_string() {
        return raw_affiliation_string;
    }

    public void setRaw_affiliation_string(String raw_affiliation_string) {
        this.raw_affiliation_string = raw_affiliation_string;
    }

    public List<Matching> getMatchings() {
        return matchings;
    }

    public void setMatchings(List<Matching> matchings) {
        this.matchings = matchings;
    }
}
