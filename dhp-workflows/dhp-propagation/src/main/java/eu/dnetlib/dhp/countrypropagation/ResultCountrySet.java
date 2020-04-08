package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;
import java.util.Set;

public class ResultCountrySet implements Serializable {
    private String resultId;
    private Set<Country> countrySet;

    public String getResultId() {
        return resultId;
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public Set<Country> getCountrySet() {
        return countrySet;
    }

    public void setCountrySet(Set<Country> countrySet) {
        this.countrySet = countrySet;
    }
}
