package eu.dnetlib.dhp.person;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.utils.DHPUtils;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

public class OrcidIndicators implements Serializable {
    private String resultId;
    private String orcid;
    private Integer downloads;
    private Integer views;
    private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class)
            + IdentifierFactory.ID_PREFIX_SEPARATOR + ModelConstants.ORCID + "_______";
    public static OrcidIndicators newInstance(String id, String orcid, List<Measure> measures) {
        OrcidIndicators oi = new OrcidIndicators();
        oi.resultId = id;
        oi.orcid = DHPUtils.generateIdentifier(orcid, PERSON_PREFIX);
        oi.downloads = measures.stream().filter(m -> m.getId().equalsIgnoreCase("downloads"))
                .findFirst()
                .map(m -> Integer.parseInt(m.getUnit().get(0).getValue()))
                .orElse(0);
        oi.views = measures.stream().filter(m -> m.getId().equalsIgnoreCase("views"))
                .findFirst()
                .map(m -> Integer.parseInt(m.getUnit().get(0).getValue()))
                .orElse(0);
        return oi;

    }

    public String getOrcid() {
        return orcid;
    }

    public void setOrcid(String orcid) {
        this.orcid = orcid;
    }

    public Integer getDownloads() {
        return downloads;
    }

    public void setDownloads(Integer downloads) {
        this.downloads = downloads;
    }

    public Integer getViews() {
        return views;
    }

    public void setViews(Integer views) {
        this.views = views;
    }

    public String getResultId() {
        return resultId;
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public void addIndicators(Integer downloads, Integer views) {
        this.downloads += downloads;
        this.views += views;
    }
}
