
package eu.dnetlib.dhp.common.person;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.apache.spark.sql.types.DataTypes.StringType;

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


	public final static StructType MATCHING_SCHEMA = new StructType()
			.add("provenance", StringType)
			.add("pid", StringType)
			.add("value", StringType)
			.add("confidence", DataTypes.DoubleType)
			.add("status", StringType)
			.add("country", StringType)
			.add("name", StringType)
			;

	public final static ArrayType PID_SCHEMA = DataTypes.createArrayType(
			new StructType()
					.add("schema", StringType)
					.add("value",StringType)
	);

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
											.add("Matchings", DataTypes.createArrayType(MATCHING_SCHEMA))
							))
							.add("corresponding", DataTypes.BooleanType)
							.add("contributor_roles", DataTypes.createArrayType(new StructType()
									.add("schema", StringType)
									.add("name", StringType)
									.add("value", StringType)))
							.add("pids", PID_SCHEMA)
			))
			.add("organizations",DataTypes.createArrayType(MATCHING_SCHEMA));

	public static final StructType OPENAPC_INPUT_SCHEMA = new StructType()
			.add("doi", StringType)
			.add("matchings", DataTypes.createArrayType(MATCHING_SCHEMA));

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

	public static List<KeyValue> getKeyValues(SerializationBean sb){
		List<KeyValue> keyValueList = new ArrayList<>();
		if(Optional.ofNullable(sb.getCorresponding()).isPresent()) {
			KeyValue kv = new KeyValue();
			kv.setKey("corresponding");
			kv.setValue(String.valueOf(sb.getCorresponding()));
			keyValueList.add(kv);
		}
		if(!sb.getAffs().isEmpty()) {
			sb.getAffs().forEach(a -> {
				KeyValue kv = new KeyValue();
				kv.setKey("declared_affiliation");
				if (Optional.ofNullable(a.getRor()).isPresent())
					kv.setValue(a.getRor());
				else
					kv.setValue("OpenOrgs: " + a.getOpenOrgs());
				kv
						.setDataInfo(
								OafMapperUtils
										.dataInfo(
												false,
												"openaire:inference",
												true,
												false,
												null,
												String.valueOf(a.getConfidence())));
				keyValueList.add(kv);
			});
		}
		if(Optional.ofNullable(sb.getRoles()).isPresent()) {
			sb.getRoles().stream().forEach(r -> {
				KeyValue kv = new KeyValue();
				if(Optional.ofNullable(r.getRoleSchema()).isPresent() && Optional.ofNullable(r.getRoleValue()).isPresent()) {
					kv.setKey("role");
					kv.setValue(r.getRoleSchema() + " " + r.getRoleValue());
				}else {
					kv.setKey("role");
					kv.setValue(r.getRoleName());
				}
				keyValueList.add(kv);
			});
		}
		return keyValueList;

	}

	public static @NotNull List<KeyValue> getKeyValues(String raf) throws IOException {
		return getKeyValues( new ObjectMapper().readValue(raf, SerializationBean.class));
	}

	public static @NotNull SerializationBean getSerializationBean(Row a) {
		List<Row> affiliations = a.getList(a.fieldIndex("affiliations"));
		SerializationBean sb = new SerializationBean();
		sb.setAffs(affiliations.stream().map(
				aff -> {
					if(aff.getAs("status").equals("active")){
						SerializationOrg so = new SerializationOrg();
						if("ror".equalsIgnoreCase(aff.getAs("pid")))
							so.setRor(aff.getAs("value"));
						else
							so.setOpenOrgs(aff.getAs("value"));
						so.setConfidence(aff.getAs("confidence"));
						return so;
					}
					return null;
				}
		).filter(Objects::nonNull).collect(Collectors.toList()));
		List<Row> roles = a.getList(a.fieldIndex("contributor_roles"));
		if(Optional.ofNullable(roles).isPresent())
			sb.setRoles(roles.stream().map(r -> {
				SerializationRoles sr = null;
				if(Optional.ofNullable(r.getAs("schema")).isPresent()){
					sr = new SerializationRoles();
					sr.setRoleSchema(r.getAs("schema"));
				}

				if(Optional.ofNullable(r.getAs("value")).isPresent()){
					if(sr == null)
						sr = new SerializationRoles();
					sr.setRoleValue(r.getAs("value"));
				}
				if(Optional.ofNullable(r.getAs("name")).isPresent()){
					if(sr == null)
						sr = new SerializationRoles();
					sr.setRoleName(r.getAs("name"));
				}
				return sr;
			}).filter(Objects::nonNull).collect(Collectors.toList()));

		if(Optional.ofNullable(a.getAs("corresponding")).isPresent())
			sb.setCorresponding(Boolean.valueOf(a.getAs("corresponding")));
		return sb;
	}
}
