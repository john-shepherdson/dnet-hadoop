package eu.dnetlib.dhp.person;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.utils.DHPUtils;

import java.io.Serializable;
import java.util.List;


public class OrcidIndicators implements Serializable {
    private String resultId;
    private String orcid;
    private Integer downloads;
    private Integer citations;

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
        oi.citations = measures.stream().filter(m -> m.getId().equalsIgnoreCase("influence_alt"))
                .findFirst()
                .map(m -> m.getUnit().stream().filter(u->u.getKey().equals("score")).findFirst()
                        .map(u -> Integer.parseInt(u.getValue()))
                        .orElse(0)
                )
                .orElse(0);
        return oi;

    }

//    public static OrcidIndicators newInstance(String id, String orcid) {
//        OrcidIndicators oi = new OrcidIndicators();
//        oi.resultId = id;
//        oi.orcid = DHPUtils.generateIdentifier(orcid, PERSON_PREFIX);
//        oi.citations = 1;
//        oi.downloads = 0;
//        return oi;
//
//    }

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



    public String getResultId() {
        return resultId;
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public void addIndicators(Integer downloads, Integer citations) {
        this.downloads += downloads;
        this.citations += citations;
    }

    public void setCitations(Integer citations) {
        this.citations = citations;
    }

    public Integer getCitations() {
        return citations;
    }
}
