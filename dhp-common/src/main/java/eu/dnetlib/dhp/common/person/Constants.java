
package eu.dnetlib.dhp.common.person;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

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

	public static final DataInfo OPENAIRE_DATAINFO = OafMapperUtils
			.dataInfo(
					false,
					null,
					false,
					false,
					OafMapperUtils
							.qualifier(
									"sysimport:crosswalk:actionset",
									"",
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
			.add("doi", StringType)
			.add(
					"authors", DataTypes
							.createArrayType(
									new StructType()
											.add("corresponding", BooleanType)
											.add(
													"contributor_roles", DataTypes
															.createArrayType(
																	new StructType()
																			.add("schema", StringType)
																			.add("value", StringType)
																			.add("name", StringType)))
											.add(
													"name", new StructType()
															.add("full",
																	StringType)
															.add("first",
																	StringType)
															.add("last", StringType))
											.add(
													"matchings", DataTypes
															.createArrayType(
																	new StructType()
																			.add("pid", StringType)
																			.add("value", StringType)
																			.add("confidence", DoubleType)
																			.add("status", StringType)
																			.add("country", StringType)
																			.add("name", StringType)))
											.add(
													"pids", DataTypes
															.createArrayType(
																	new StructType()
																			.add("schema", StringType)
																			.add("value", StringType)))));

	public final static StructType MATCHING_SCHEMA = new StructType()
			.add("provenance", StringType)
			.add("pid", StringType)
			.add("value", StringType)
			.add("confidence", DataTypes.DoubleType)
			.add("status", StringType)
			.add("country", StringType)
			.add("name", StringType)
			;


	public final static StructType PID_STRUCT = new StructType()
			.add("schema", StringType)
			.add("value", StringType);


	public final static ArrayType PID_SCHEMA = DataTypes.createArrayType(PID_STRUCT);
	public final static ArrayType MATCHING_ARRAY_SCHEMA =  DataTypes.createArrayType(MATCHING_SCHEMA);

	public final static StructType RESULT_MATCHED_SCHEMA = new StructType()
			.add("id", StringType)
			.add("authors", DataTypes.createArrayType(
					new StructType()
							.add("fullname", StringType)
							.add("firstname", StringType)
							.add("lastname", StringType)
							.add("affiliations", DataTypes.createArrayType(
									new StructType()
											.add("raw_affiliation_string", StringType)
											.add("matchings", MATCHING_ARRAY_SCHEMA)
							))
							.add("corresponding", DataTypes.BooleanType)
							.add("contributor_roles", DataTypes.createArrayType(new StructType()
									.add("schema", StringType)
									.add("name", StringType)
									.add("value", StringType)))
							.add("pids", PID_SCHEMA)
			))
			.add("organizations",MATCHING_ARRAY_SCHEMA);

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

	public static String getPersonId(String pid){
		return DHPUtils.generateIdentifier(removePrefixUrl(pid).toUpperCase(), PERSON_PREFIX);
	}
}
