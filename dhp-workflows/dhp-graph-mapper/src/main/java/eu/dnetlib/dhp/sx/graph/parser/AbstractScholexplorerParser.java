
package eu.dnetlib.dhp.sx.graph.parser;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.scholexplorer.DLIRelation;
import eu.dnetlib.dhp.schema.scholexplorer.DLIUnknown;
import eu.dnetlib.dhp.schema.scholexplorer.ProvenaceInfo;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.scholexplorer.relation.RelInfo;
import eu.dnetlib.scholexplorer.relation.RelationMapper;

public abstract class AbstractScholexplorerParser {

	protected static final Log log = LogFactory.getLog(AbstractScholexplorerParser.class);
	static final Pattern pattern = Pattern.compile("10\\.\\d{4,9}/[-._;()/:A-Z0-9]+$", Pattern.CASE_INSENSITIVE);
	private List<String> datasetSubTypes = Arrays
		.asList(
			"dataset",
			"software",
			"film",
			"sound",
			"physicalobject",
			"audiovisual",
			"collection",
			"other",
			"study",
			"metadata");

	public abstract List<Oaf> parseObject(final String record, final RelationMapper relMapper);

	protected Map<String, String> getAttributes(final XMLStreamReader parser) {
		final Map<String, String> attributesMap = new HashMap<>();
		for (int i = 0; i < parser.getAttributeCount(); i++) {
			attributesMap.put(parser.getAttributeLocalName(i), parser.getAttributeValue(i));
		}
		return attributesMap;
	}

	protected List<StructuredProperty> extractSubject(List<VtdUtilityParser.Node> subjects) {
		final List<StructuredProperty> subjectResult = new ArrayList<>();
		if (subjects != null && subjects.size() > 0) {
			subjects
				.forEach(
					subjectMap -> {
						final StructuredProperty subject = new StructuredProperty();
						subject.setValue(subjectMap.getTextValue());
						final Qualifier schema = new Qualifier();
						schema.setClassid("dnet:subject");
						schema.setClassname("dnet:subject");
						schema.setSchemeid(subjectMap.getAttributes().get("subjectScheme"));
						schema.setSchemename(subjectMap.getAttributes().get("subjectScheme"));
						subject.setQualifier(schema);
						subjectResult.add(subject);
					});
		}
		return subjectResult;
	}

	protected StructuredProperty extractIdentifier(
		List<VtdUtilityParser.Node> identifierType, final String fieldName) {
		final StructuredProperty pid = new StructuredProperty();
		if (identifierType != null && identifierType.size() > 0) {
			final VtdUtilityParser.Node result = identifierType.get(0);
			pid.setValue(result.getTextValue());
			final Qualifier pidType = new Qualifier();
			pidType.setClassname(result.getAttributes().get(fieldName));
			pidType.setClassid(result.getAttributes().get(fieldName));
			pidType.setSchemename("dnet:pid_types");
			pidType.setSchemeid("dnet:pid_types");
			pid.setQualifier(pidType);
			return pid;
		}
		return null;
	}

	protected void inferPid(final StructuredProperty input) {
		final Matcher matcher = pattern.matcher(input.getValue());
		if (matcher.find()) {
			input.setValue(matcher.group());
			if (input.getQualifier() == null) {
				input.setQualifier(new Qualifier());
				input.getQualifier().setSchemename("dnet:pid_types");
				input.getQualifier().setSchemeid("dnet:pid_types");
			}
			input.getQualifier().setClassid("doi");
			input.getQualifier().setClassname("doi");
		}
	}

	protected String generateId(final String pid, final String pidType, final String entityType) {
		String type;
		switch (entityType) {
			case "publication":
				type = "50|";
				break;
			case "dataset":
				type = "60|";
				break;
			case "unknown":
				type = "70|";
				break;
			default:
				throw new IllegalArgumentException("unexpected value " + entityType);
		}
		if ("dnet".equalsIgnoreCase(pidType))
			return type + StringUtils.substringAfter(pid, "::");

		return type
			+ DHPUtils
				.md5(
					String.format("%s::%s", pid.toLowerCase().trim(), pidType.toLowerCase().trim()));
	}

	protected DLIUnknown createUnknownObject(
		final String pid,
		final String pidType,
		final KeyValue cf,
		final DataInfo di,
		final String dateOfCollection) {
		final DLIUnknown uk = new DLIUnknown();
		uk.setId(generateId(pid, pidType, "unknown"));
		ProvenaceInfo pi = new ProvenaceInfo();
		pi.setId(cf.getKey());
		pi.setName(cf.getValue());
		pi.setCompletionStatus("incomplete");
		uk.setDataInfo(di);
		uk.setDlicollectedfrom(Collections.singletonList(pi));
		final StructuredProperty sourcePid = new StructuredProperty();
		sourcePid.setValue(pid);
		final Qualifier pt = new Qualifier();
		pt.setClassname(pidType);
		pt.setClassid(pidType);
		pt.setSchemename("dnet:pid_types");
		pt.setSchemeid("dnet:pid_types");
		sourcePid.setQualifier(pt);
		uk.setPid(Collections.singletonList(sourcePid));
		uk.setDateofcollection(dateOfCollection);
		return uk;
	}

	protected void generateRelations(
		RelationMapper relationMapper,
		Result parsedObject,
		List<Oaf> result,
		DataInfo di,
		String dateOfCollection,
		List<VtdUtilityParser.Node> relatedIdentifiers) {
		if (relatedIdentifiers != null) {
			result
				.addAll(
					relatedIdentifiers
						.stream()
						.flatMap(
							n -> {
								final List<DLIRelation> rels = new ArrayList<>();
								DLIRelation r = new DLIRelation();
								r.setSource(parsedObject.getId());
								final String relatedPid = n.getTextValue();
								final String relatedPidType = n.getAttributes().get("relatedIdentifierType");
								final String relatedType = n.getAttributes().getOrDefault("entityType", "unknown");
								String relationSemantic = n.getAttributes().get("relationType");
								String inverseRelation;
								final String targetId = generateId(relatedPid, relatedPidType, relatedType);
								r.setDateOfCollection(dateOfCollection);
								if (relationMapper.containsKey(relationSemantic.toLowerCase())) {
									RelInfo relInfo = relationMapper.get(relationSemantic.toLowerCase());
									relationSemantic = relInfo.getOriginal();
									inverseRelation = relInfo.getInverse();
								} else {
									relationSemantic = "Unknown";
									inverseRelation = "Unknown";
								}
								r.setTarget(targetId);
								r.setRelType(relationSemantic);
								r.setRelClass("datacite");
								r.setCollectedfrom(parsedObject.getCollectedfrom());
								r.setDataInfo(di);
								rels.add(r);
								r = new DLIRelation();
								r.setDataInfo(di);
								r.setSource(targetId);
								r.setTarget(parsedObject.getId());
								r.setRelType(inverseRelation);
								r.setRelClass("datacite");
								r.setCollectedfrom(parsedObject.getCollectedfrom());
								r.setDateOfCollection(dateOfCollection);
								rels.add(r);
								if ("unknown".equalsIgnoreCase(relatedType))
									result
										.add(
											createUnknownObject(
												relatedPid,
												relatedPidType,
												parsedObject.getCollectedfrom().get(0),
												di,
												dateOfCollection));
								return rels.stream();
							})
						.collect(Collectors.toList()));
		}
	}
}
