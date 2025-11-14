
package eu.dnetlib.dhp.common.person;

import java.util.List;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

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
	public static final String ORCID_KEY = ModelSupport.getIdPrefix(Datasource.class) +
		IdentifierFactory.ID_PREFIX_SEPARATOR + OPENAIRE_PREFIX + SEPARATOR
		+ DHPUtils.md5(ModelConstants.ORCID.toLowerCase());
	public static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class)
		+ IdentifierFactory.ID_PREFIX_SEPARATOR +
		"orcid_______";
	public static final String PROJECT_ID_PREFIX = ModelSupport.getIdPrefix(Project.class)
		+ IdentifierFactory.ID_PREFIX_SEPARATOR;

	public static final StructType PUBLISHER_INPUT_SCHEMA = new StructType()
			.add("doi", DataTypes.StringType)
			.add(
					"authors", DataTypes
							.createArrayType(
									new StructType()
											.add("corresponding", DataTypes.BooleanType)
											.add(
													"contributor_roles", DataTypes
															.createArrayType(
																	new StructType()
																			.add("schema", DataTypes.StringType)
																			.add("value", DataTypes.StringType)
																			.add("name", DataTypes.StringType)))
											.add(
													"name", new StructType()
															.add("full", DataTypes.StringType)
															.add("first", DataTypes.StringType)
															.add("last", DataTypes.StringType))
											.add(
													"matchings", DataTypes
															.createArrayType(
																	new StructType()
																			.add("PID", DataTypes.StringType)
																			.add("Value", DataTypes.StringType)
																			.add("Confidence", DataTypes.DoubleType)
																			.add("Status", DataTypes.StringType)))
											.add(
													"pids", DataTypes
															.createArrayType(
																	new StructType()
																			.add("schema", DataTypes.StringType)
																			.add("value", DataTypes.StringType)))));

	public static String removePrefixUrl(String pid) {
		if (pid == null) {
			return null;
		}

		String trimmed = pid.trim();

		// removes prefix for DOI
		if (trimmed.matches("(?i)^https?://(dx\\.)?doi\\.org/.*")) {
			return trimmed.replaceFirst("(?i)^https?://(dx\\.)?doi\\.org/", "");
		}

		// removes prefix for ORCID
		if (trimmed.matches("(?i)^https?://orcid\\.org/.*")) {
			return trimmed.replaceFirst("(?i)^https?://orcid\\.org/", "");
		}

		// if there is no known prefix to remove the string is returned as it is
		return trimmed;
	}
}
