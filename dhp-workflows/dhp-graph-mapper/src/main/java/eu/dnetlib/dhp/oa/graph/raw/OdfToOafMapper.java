
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.structuredProperty;

import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.common.RelationInverse;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;

public class OdfToOafMapper extends AbstractMdRecordToOafMapper {

	public static final String HTTP_DOI_PREIFX = "https://doi.org/";
	public static final String HTTP_HANDLE_PREIFX = "https://hdl.handle.net/";

	public OdfToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId,
		final boolean forceOrginalId) {
		super(vocs, invisible, shouldHashId, forceOrginalId);
	}

	public OdfToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId) {
		super(vocs, invisible, shouldHashId);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {

		final List<StructuredProperty> title = Lists.newArrayList();
		final String xpath = "//*[local-name()='titles']/*[local-name()='title']|//*[local-name()='resource']/*[local-name()='title']";

		for (Object o : doc.selectNodes(xpath)) {
			Element e = (Element) o;
			final String titleValue = e.getTextTrim();
			final String titleType = e.attributeValue("titleType");
			if (StringUtils.isNotBlank(titleType)) {
				title
					.add(
						structuredProperty(
							titleValue, titleType, titleType, DNET_DATACITE_TITLE, DNET_DATACITE_TITLE, info));
			} else {
				title.add(structuredProperty(titleValue, MAIN_TITLE_QUALIFIER, info));
			}
		}

		return title;
	}

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
		final List<Author> res = new ArrayList<>();
		int pos = 1;
		for (final Object o : doc.selectNodes("//*[local-name()='creator']")) {
			final Node n = (Node) o;
			final Author author = new Author();
			final String fullname = n.valueOf("./*[local-name()='creatorName']");
			final String name = n.valueOf("./*[local-name()='givenName']");
			final String surname = n.valueOf("./*[local-name()='familyName']");
			if (StringUtils.isNotBlank(fullname) || StringUtils.isNotBlank(name) || StringUtils.isNotBlank(surname)) {
				author.setFullname(fullname);

				final PacePerson pp = new PacePerson(fullname, false);

				if (StringUtils.isBlank(name) & pp.isAccurate()) {
					author.setName(pp.getNormalisedFirstName());
				} else {
					author.setName(name);
				}

				if (StringUtils.isBlank(surname) & pp.isAccurate()) {
					author.setSurname(pp.getNormalisedSurname());
				} else {
					author.setSurname(surname);
				}

				if (StringUtils.isBlank(author.getFullname())) {
					author.setFullname(String.format("%s, %s", author.getSurname(), author.getName()));
				}

				author.setRawAffiliationString(prepareListString(n, "./*[local-name()='affiliation']"));
				author.setPid(preparePids(n, info));
				author.setRank(pos++);
				res.add(author);
			}
		}
		return res;
	}

	private List<StructuredProperty> preparePids(final Node n, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : n.selectNodes("./*[local-name()='nameIdentifier']")) {

			final String id = ((Node) o).getText();
			final String type = ((Node) o)
				.valueOf("./@nameIdentifierScheme")
				.trim()
				.toUpperCase()
				.replace(" ", "")
				.replace("_", "");

			if (type.toLowerCase().startsWith(ORCID)) {
				final String cleanedId = id.replace("http://orcid.org/", "").replace("https://orcid.org/", "");
				res.add(structuredProperty(cleanedId, ORCID_PID_TYPE, info));
			} else if (type.startsWith("MAGID")) {
				res.add(structuredProperty(id, MAG_PID_TYPE, info));
			}
		}
		return res;
	}

	@Override
	protected Instance prepareInstances(
		final Document doc,
		final DataInfo info,
		final KeyValue collectedfrom,
		final KeyValue hostedby) {

		final Instance instance = new Instance();
		instance
			.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", DNET_PUBLICATION_RESOURCE));
		instance.setCollectedfrom(collectedfrom);
		instance.setHostedby(hostedby);

		final List<StructuredProperty> alternateIdentifier = prepareResultPids(doc, info);
		final List<StructuredProperty> pid = IdentifierFactory.getPids(alternateIdentifier, collectedfrom);

		instance.setInstanceTypeMapping(prepareInstanceTypeMapping(doc));

		final Set<StructuredProperty> pids = new HashSet<>(pid);

		instance
			.setAlternateIdentifier(
				alternateIdentifier.stream().filter(i -> !pids.contains(i)).collect(Collectors.toList()));
		instance.setPid(pid);

		instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
		final String distributionlocation = doc.valueOf("//oaf:distributionlocation");
		instance.setDistributionlocation(StringUtils.isNotBlank(distributionlocation) ? distributionlocation : null);
		instance
			.setAccessright(prepareAccessRight(doc, "//oaf:accessrights", DNET_ACCESS_MODES));
		instance.setLicense(field(doc.valueOf("//oaf:license"), info));
		instance.setRefereed(prepareQualifier(doc, "//oaf:refereed", DNET_REVIEW_LEVELS));
		instance.setProcessingchargeamount(field(doc.valueOf("//oaf:processingchargeamount"), info));
		instance
			.setProcessingchargecurrency(field(doc.valueOf("//oaf:processingchargeamount/@currency"), info));
		prepareListURL(doc, "//oaf:fulltext", info)
			.stream()
			.findFirst()
			.map(Field::getValue)
			.ifPresent(instance::setFulltext);

		final Set<String> url = new HashSet<>();
		for (final Object o : doc
			.selectNodes("//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType='URL']")) {
			url.add(trimAndDecodeUrl(((Node) o).getText().trim()));
		}
		for (final Object o : doc
			.selectNodes("//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType='landingPage']")) {
			url.add(trimAndDecodeUrl(((Node) o).getText().trim()));
		}
		for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='URL']")) {
			url.add(trimAndDecodeUrl(((Node) o).getText().trim()));
		}
		for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='landingPage']")) {
			url.add(trimAndDecodeUrl(((Node) o).getText().trim()));
		}
		for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='w3id']")) {
			url.add(trimAndDecodeUrl(((Node) o).getText().trim()));
		}

		Set<String> validUrl = validateUrl(url);

		if (validUrl.stream().noneMatch(s -> s.contains("doi.org"))) {
			for (final Object o : doc
				.selectNodes("//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType='DOI']")) {
				validUrl.add(HTTP_DOI_PREIFX + ((Node) o).getText().trim());
			}
			for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='DOI']")) {
				validUrl.add(HTTP_DOI_PREIFX + ((Node) o).getText().trim());
			}
		}
		if (validUrl.stream().noneMatch(s -> s.contains("hdl.handle.net"))) {
			for (final Object o : doc
				.selectNodes("//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType='Handle']")) {
				validUrl.add(HTTP_HANDLE_PREIFX + ((Node) o).getText().trim());
			}
			for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='Handle']")) {
				validUrl.add(HTTP_HANDLE_PREIFX + ((Node) o).getText().trim());
			}
		}

		if (!validUrl.isEmpty()) {
			instance.setUrl(new ArrayList<>());
			instance.getUrl().addAll(validUrl);
		}
		return instance;
	}

	protected String trimAndDecodeUrl(String url) {
		try {
			return URLDecoder.decode(url.trim(), "UTF-8");
		} catch (Throwable t) {
			return url;
		}
	}

	/**
	 * Extracts the resource type from The Datacite element
	 *
	 * <datacite:resourceType
	 * 		anyURI="http://purl.org/coar/resource_type/c_6501"
	 * 		uri="http://purl.org/coar/resource_type/c_6501"
	 * 	    resourceTypeGeneral="Dataset">journal article</datacite:resourceType>
	 *
	 * @param doc the input document
	 * @return the chosen resource type
	 */
	@Override
	protected String findOriginalType(Document doc) {
		final String resourceType = Optional
			.ofNullable(
				(Element) doc
					.selectSingleNode(
						"//*[local-name()='metadata']/*[local-name() = 'resource']/*[local-name() = 'resourceType']"))
			.map(e -> {
				final String resourceTypeURI = Optional
					.ofNullable(e.attributeValue("uri"))
					.filter(StringUtils::isNotBlank)
					.orElse(null);
				final String resourceTypeAnyURI = Optional
					.ofNullable(e.attributeValue("anyURI"))
					.filter(StringUtils::isNotBlank)
					.orElse(null);
				final String resourceTypeTxt = Optional
					.ofNullable(e.getText())
					.filter(StringUtils::isNotBlank)
					.orElse(null);
				final String resourceTypeGeneral = Optional
					.ofNullable(e.attributeValue("resourceTypeGeneral"))
					.filter(StringUtils::isNotBlank)
					.orElse(null);

				return ObjectUtils
					.firstNonNull(resourceTypeURI, resourceTypeAnyURI, resourceTypeTxt, resourceTypeGeneral);
			})
			.orElse(null);

		final String drCobjCategory = doc.valueOf("//dr:CobjCategory/text()");
		return ObjectUtils.firstNonNull(resourceType, drCobjCategory);
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//*[local-name()='date']")) {
			final String dateType = ((Node) o).valueOf("@dateType");
			if (StringUtils.isBlank(dateType)
				|| (!dateType.equalsIgnoreCase("Accepted")
					&& !dateType.equalsIgnoreCase("Issued")
					&& !dateType.equalsIgnoreCase("Updated")
					&& !dateType.equalsIgnoreCase("Available"))) {
				res
					.add(
						structuredProperty(
							((Node) o).getText(), UNKNOWN, UNKNOWN, DNET_DATACITE_DATE, DNET_DATACITE_DATE,
							info));
			} else {
				res
					.add(
						structuredProperty(
							((Node) o).getText(), dateType, dateType, DNET_DATACITE_DATE, DNET_DATACITE_DATE,
							info));
			}
		}
		return res;
	}

	@Override
	protected List<Field<String>> prepareCoverages(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareContributors(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//*[local-name()='contributorName']", info);
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//*[local-name()='format']", info);
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		return prepareField(doc, "//*[local-name()='publisher']", info);
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//datacite:description[./@descriptionType='Abstract'] | //dc:description", info);
	}

	@Override
	protected List<Subject> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareSubjectList(doc, "//*[local-name()='subject']", info);
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//*[local-name()='language']", DNET_LANGUAGES);
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductTools(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactGroups(
		final Document doc,
		final DataInfo info) {
		return prepareListFields(
			doc,
			"//*[local-name()='contributor' and ./@contributorType='ContactGroup']/*[local-name()='contributorName']",
			info);
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(
		final Document doc,
		final DataInfo info) {
		return prepareListFields(
			doc,
			"//*[local-name()='contributor' and ./@contributorType='ContactPerson']/*[local-name()='contributorName']",
			info);
	}

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc, final DataInfo info) {
		return prepareQualifier(doc, "//*[local-name()='format']", DNET_PROGRAMMING_LANGUAGES);
	}

	@Override
	protected Field<String> prepareSoftwareCodeRepositoryUrl(
		final Document doc,
		final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareSoftwareLicenses(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<Field<String>> prepareSoftwareDocumentationUrls(
		final Document doc,
		final DataInfo info) {
		return prepareListFields(
			doc,
			"//*[local-name()='relatedIdentifier' and ./@relatedIdentifierType='URL' and @relationType='IsDocumentedBy']",
			info);
	}

	// DATASETS

	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		final List<GeoLocation> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//*[local-name()='geoLocation']")) {
			final GeoLocation loc = new GeoLocation();
			loc.setBox(((Node) o).valueOf("./*[local-name()='geoLocationBox']"));
			loc.setPlace(((Node) o).valueOf("./*[local-name()='geoLocationPlace']"));
			loc.setPoint(((Node) o).valueOf("./*[local-name()='geoLocationPoint']"));
			res.add(loc);
		}
		return res;
	}

	@Override
	protected Field<String> prepareDatasetMetadataVersionNumber(
		final Document doc,
		final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected Field<String> prepareDatasetLastMetadataUpdate(
		final Document doc,
		final DataInfo info) {
		return prepareField(doc, "//*[local-name()='date' and ./@dateType='Updated']", info);
	}

	@Override
	protected Field<String> prepareDatasetVersion(final Document doc, final DataInfo info) {
		return prepareField(doc, "//*[local-name()='version']", info);
	}

	@Override
	protected Field<String> prepareDatasetSize(final Document doc, final DataInfo info) {
		return prepareField(doc, "//*[local-name()='size']", info);
	}

	@Override
	protected Field<String> prepareDatasetDevice(final Document doc, final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected Field<String> prepareDatasetStorageDate(final Document doc, final DataInfo info) {
		return prepareField(doc, "//*[local-name()='date' and ./@dateType='Issued']", info);
	}

	@Override
	protected List<Oaf> addOtherResultRels(
		final Document doc,
		final OafEntity entity, DataInfo info) {

		final String docId = entity.getId();

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc
			.selectNodes("//*[local-name()='relatedIdentifier']")) {

			final String originalId = ((Node) o).getText().trim();

			if (StringUtils.isNotBlank(originalId)) {
				final String idType = ((Node) o).valueOf("@relatedIdentifierType");
				final String relType = ((Node) o).valueOf("@relationType");
				String otherId = guessRelatedIdentifier(idType, originalId);
				if (StringUtils.isNotBlank(otherId)) {
					res.addAll(getRelations(relType, docId, otherId, entity, info));
				}

			}
		}
		return res;
	}

	protected String guessRelatedIdentifier(final String idType, final String value) {
		if (StringUtils.isBlank(idType) || StringUtils.isBlank(value))
			return null;
		if (idType.equalsIgnoreCase("OPENAIRE"))
			return createOpenaireId(50, value, false);
		if (pidTypeWithAuthority.containsKey(idType.toLowerCase())) {
			return IdentifierFactory.idFromPid("50", pidTypeWithAuthority.get(idType.toLowerCase()), value, true);
		}
		return null;

	}

	protected List<Oaf> getRelations(final String reltype, final String entityId, final String otherId,
		final OafEntity entity, DataInfo info) {
		final List<Oaf> res = new ArrayList<>();
		RelationInverse rel = ModelSupport.findRelation(reltype);
		if (rel != null) {
			res
				.add(
					getRelation(
						entityId, otherId, rel.getRelType(), rel.getSubReltype(), rel.getRelClass(),
						entity.getCollectedfrom(), info, entity.getLastupdatetimestamp(), null, null));
			res
				.add(
					getRelation(
						otherId, entityId, rel.getRelType(), rel.getSubReltype(), rel.getInverseRelClass(),
						entity.getCollectedfrom(), info, entity.getLastupdatetimestamp(), null, null));

		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc, final DataInfo info) {
		return prepareQualifier(
			doc, "//*[local-name() = 'resource']//*[local-name() = 'resourceType']", DNET_DATA_CITE_RESOURCE);
	}

	@Override
	protected List<StructuredProperty> prepareResultPids(final Document doc, final DataInfo info) {
		final Set<HashableStructuredProperty> res = new HashSet<>();
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc, "//oaf:identifier", "@identifierType", DNET_PID_TYPES, info));
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc,
					"//*[local-name()='identifier' and ./@identifierType != 'URL' and ./@identifierType != 'landingPage']",
					"@identifierType", DNET_PID_TYPES, info));
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc,
					"//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType != 'URL' and ./@alternateIdentifierType != 'landingPage']",
					"@alternateIdentifierType", DNET_PID_TYPES, info));

		return res
			.stream()
			.map(PidCleaner::normalizePidValue)
			.filter(CleaningFunctions::pidFilter)
			.collect(Collectors.toList());
	}

}
