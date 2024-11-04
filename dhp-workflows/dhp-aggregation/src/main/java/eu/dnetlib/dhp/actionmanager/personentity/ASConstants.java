
package eu.dnetlib.dhp.actionmanager.personentity;

import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class ASConstants {

	public static final String DOI_PREFIX = "50|doi_________::";

	public static final String PMID_PREFIX = "50|pmid________::";
	public static final String ARXIV_PREFIX = "50|arXiv_______::";

	public static final String PMCID_PREFIX = "50|pmcid_______::";
	public static final String ROR_PREFIX = "20|ror_________::";
	public static final String OPENORGS_PREFIX = "20|openorgs____::";

	public static final String FUNDER_AUTHORS_CLASSID = "sysimport:crosswalk:funderdatabase";
	public static final String FUNDER_AUTHORS_CLASSNAME = "Imported from Funder Database";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";

	public static List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
		.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

	public static final DataInfo FUNDERDATAINFO = OafMapperUtils
		.dataInfo(
			false,
			null,
			false,
			false,
			OafMapperUtils
				.qualifier(
					FUNDER_AUTHORS_CLASSID,
					FUNDER_AUTHORS_CLASSNAME,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.91");
}
