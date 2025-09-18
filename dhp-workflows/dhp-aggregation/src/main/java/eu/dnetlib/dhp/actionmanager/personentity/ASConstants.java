
package eu.dnetlib.dhp.actionmanager.personentity;

import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ASConstants {

	public static final String QUERY = "SELECT * FROM project_person WHERE pid_type = 'ORCID'";

	public static final String ROR_PREFIX = "20|ror_________::";

	public static final String FUNDER_AUTHORS_CLASSID = "sysimport:crosswalk:funderdatabase";
	public static final String FUNDER_AUTHORS_CLASSNAME = "Imported from Funder Database";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";

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

	public static final List<KeyValue> OPENAIRE_COLLECTED_FROM = OafMapperUtils
		.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

	public static final StructType schema = new StructType()
			.add("id", DataTypes.StringType)
			.add("authors", DataTypes.createArrayType(
					new StructType()
							.add("corresponding", DataTypes.StringType)
							.add("contributor_roles", DataTypes.createArrayType(
									new StructType()
											.add("schema", DataTypes.StringType)
											.add("value", DataTypes.StringType)
											.add("name", DataTypes.StringType)))
							.add("affiliations", DataTypes.createArrayType(
									new StructType()
											.add("raw_affiliation_string", DataTypes.StringType)
											.add("Matchings", DataTypes.createArrayType(
													new StructType()
															.add("pid", DataTypes.StringType)
															.add("value", DataTypes.StringType)
															.add("name", DataTypes.StringType)
															.add("confidence", DataTypes.DoubleType)
															.add("status", DataTypes.StringType)
															.add("country", DataTypes.StringType)))))
							.add("pids", DataTypes.createArrayType(
									new StructType()
											.add("schema", DataTypes.StringType)
											.add("value", DataTypes.StringType)))
			));

}
