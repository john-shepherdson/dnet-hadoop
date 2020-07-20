package eu.dnetlib.dhp.schema.dump.oaf.graph;

import eu.dnetlib.dhp.schema.dump.oaf.ControlledField;
import eu.dnetlib.dhp.schema.dump.oaf.Country;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

import java.io.Serializable;
import java.util.List;

public class Organization implements Serializable {
    private String legalshortname;
    private String legalname;
    private String websiteurl;
    private List<String> alternativenames;
    private Country country;
    private String id;
    private List<ControlledField> pid;
    private String collectedfrom;

    public String getLegalshortname() {
        return legalshortname;
    }

    public void setLegalshortname(String legalshortname) {
        this.legalshortname = legalshortname;
    }

    public String getLegalname() {
        return legalname;
    }

    public void setLegalname(String legalname) {
        this.legalname = legalname;
    }

    public String getWebsiteurl() {
        return websiteurl;
    }

    public void setWebsiteurl(String websiteurl) {
        this.websiteurl = websiteurl;
    }

    public List<String> getAlternativenames() {
        return alternativenames;
    }

    public void setAlternativenames(List<String> alternativenames) {
        this.alternativenames = alternativenames;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<ControlledField> getPid() {
        return pid;
    }

    public void setPid(List<ControlledField> pid) {
        this.pid = pid;
    }

    public String getCollectedfrom() {
        return collectedfrom;
    }

    public void setCollectedfrom(String collectedfrom) {
        this.collectedfrom = collectedfrom;
    }
}
