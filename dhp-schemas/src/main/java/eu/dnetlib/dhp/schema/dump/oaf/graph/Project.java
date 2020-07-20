package eu.dnetlib.dhp.schema.dump.oaf.graph;





import java.io.Serializable;
import java.util.List;

public class Project implements Serializable {
    private String id;
    private String websiteurl;
    private String code;
    private String acronym;
    private String title;
    private String startdate;

    private String enddate;

    private String callidentifier;

    private String keywords;

    private String duration;

    private boolean openaccessmandateforpublications;

    private boolean openaccessmandatefordataset;
    private List<String> subject;
    private Funder funding;

    private String summary;

    private Granted granted;

    private Programme programme;

}
