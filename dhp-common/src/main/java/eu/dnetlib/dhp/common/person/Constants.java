package eu.dnetlib.dhp.common.person;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;

import java.util.List;

public class Constants {
    public static final String ORCID_AUTHORS_CLASSID = "sysimport:crosswalk:orcid";
    public static final String ORCID_AUTHORS_CLASSNAME = "Imported from ORCID";
    public static final DataInfo ORCIDDATAINFO = OafMapperUtils
            .dataInfo(
                    false,
                    null,
                    false,
                    false,
                    OafMapperUtils
                            .qualifier(
                                    ORCID_AUTHORS_CLASSID,
                                    ORCID_AUTHORS_CLASSNAME,
                                    ModelConstants.DNET_PROVENANCE_ACTIONS,
                                    ModelConstants.DNET_PROVENANCE_ACTIONS),
                    "0.91");

    public static final String OPENAIRE_PREFIX = "openaire____";
    public static final String SEPARATOR = "::";
    public static final String ORCID_KEY = "10|" + OPENAIRE_PREFIX + SEPARATOR
            + DHPUtils.md5(ModelConstants.ORCID.toLowerCase());
    public static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______";

}
