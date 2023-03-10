
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.structuredProperty;
import static eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory.*;

import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.common.RelationInverse;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;

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
	protected List<StructuredProperty> prepareTitles(final Document doc) {

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
							titleValue, titleType, titleType, DNET_DATACITE_TITLE));
			} else {
				title.add(structuredProperty(titleValue, MAIN_TITLE_QUALIFIER));
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

				author.setPid(preparePids(n, info));
				author.setRank(pos++);
				res.add(author);
			}
		}
		return res;
	}

	private List<AuthorPid> preparePids(final Node n, final DataInfo info) {
		final List<AuthorPid> res = new ArrayList<>();
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
				res.add(authorPid(cleanedId, ORCID_PID_TYPE, info));
			} else if (type.startsWith("MAGID")) {
				res.add(authorPid(id, MAG_PID_TYPE, info));
			}
		}
		return res;
	}

	@Override
	protected List<Instance> prepareInstances(
		final Document doc,
		final KeyValue collectedfrom,
		final KeyValue hostedby) {

		final Instance instance = new Instance();
		instance
			.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", DNET_PUBLICATION_RESOURCE));
		instance.setCollectedfrom(collectedfrom);
		instance.setHostedby(hostedby);

		final List<StructuredProperty> alternateIdentifier = prepareResultPids(doc);
		final List<StructuredProperty> pid = IdentifierFactory.getPids(alternateIdentifier, collectedfrom);

		final Set<StructuredProperty> pids = pid.stream().collect(Collectors.toCollection(HashSet::new));

		instance
			.setAlternateIdentifier(
				alternateIdentifier.stream().filter(i -> !pids.contains(i)).collect(Collectors.toList()));
		instance.setPid(pid);

		instance.setDateofacceptance(doc.valueOf("//oaf:dateAccepted"));
		final String distributionlocation = doc.valueOf("//oaf:distributionlocation");
		instance.setDistributionlocation(StringUtils.isNotBlank(distributionlocation) ? distributionlocation : null);
		instance
			.setAccessright(prepareAccessRight(doc, "//oaf:accessrights", DNET_ACCESS_MODES));
		instance.setLicense(license(doc.valueOf("//oaf:license")));
		instance.setRefereed(prepareQualifier(doc, "//oaf:refereed", DNET_REVIEW_LEVELS));
		instance.setProcessingchargeamount(doc.valueOf("//oaf:processingchargeamount"));
		instance
			.setProcessingchargecurrency(doc.valueOf("//oaf:processingchargeamount/@currency"));

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
		return Arrays.asList(instance);
	}

	protected String trimAndDecodeUrl(String url) {
		try {
			return URLDecoder.decode(url.trim(), "UTF-8");
		} catch (Throwable t) {
			return url;
		}
	}

	@Override
	protected List<String> prepareSources(final Document doc) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc) {
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
							((Node) o).getText(), UNKNOWN, UNKNOWN, DNET_DATACITE_DATE));
			} else {
				res
					.add(
						structuredProperty(
							((Node) o).getText(), dateType, dateType, DNET_DATACITE_DATE));
			}
		}
		return res;
	}

	@Override
	protected List<String> prepareCoverages(final Document doc) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<String> prepareContributors(final Document doc) {
		return prepareListFields(doc, "//*[local-name()='contributorName']");
	}

	@Override
	protected List<String> prepareFormats(final Document doc) {
		return prepareListFields(doc, "//*[local-name()='format']");
	}

	@Override
	protected Publisher preparePublisher(final Document doc) {
		return publisher(doc.valueOf("//*[local-name()='publisher']"));
	}

	@Override
	protected List<String> prepareDescriptions(final Document doc) {
		return prepareListFields(doc, "//*[local-name()='description' and ./@descriptionType='Abstract']");
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
	protected List<String> prepareOtherResearchProductTools(final Document doc) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<String> prepareOtherResearchProductContactGroups(final Document doc) {
		return prepareListFields(
			doc,
			"//*[local-name()='contributor' and ./@contributorType='ContactGroup']/*[local-name()='contributorName']");
	}

	@Override
	protected List<String> prepareOtherResearchProductContactPersons(
		final Document doc) {
		return prepareListFields(
			doc,
			"//*[local-name()='contributor' and ./@contributorType='ContactPerson']/*[local-name()='contributorName']");
	}

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc) {
		return prepareQualifier(doc, "//*[local-name()='format']", DNET_PROGRAMMING_LANGUAGES);
	}

	@Override
	protected String prepareSoftwareCodeRepositoryUrl(final Document doc) {
		return null; // Not present in ODF ???
	}

	@Override
	protected List<String> prepareSoftwareDocumentationUrls(final Document doc) {
		return prepareListFields(
			doc,
			"//*[local-name()='relatedIdentifier' and ./@relatedIdentifierType='URL' and @relationType='IsDocumentedBy']");
	}

	// DATASETS

	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc) {
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
	protected String prepareDatasetMetadataVersionNumber(final Document doc) {
		return null; // Not present in ODF ???
	}

	@Override
	protected String prepareDatasetLastMetadataUpdate(final Document doc) {
		return doc.valueOf("//*[local-name()='date' and ./@dateType='Updated']");
	}

	@Override
	protected String prepareDatasetVersion(final Document doc) {
		return doc.valueOf("//*[local-name()='version']");
	}

	@Override
	protected String prepareDatasetSize(final Document doc) {
		return doc.valueOf("//*[local-name()='size']");
	}

	@Override
	protected String prepareDatasetDevice(final Document doc) {
		return null; // Not present in ODF ???
	}

	@Override
	protected String prepareDatasetStorageDate(final Document doc) {
		return doc.valueOf("//*[local-name()='date' and ./@dateType='Issued']");
	}

	@Override
	protected List<Oaf> addOtherResultRels(
		final Document doc,
		final Entity entity) {

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
					res.addAll(getRelations(relType, docId, otherId, entity));
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
		final Entity entity) {
		final List<Oaf> res = new ArrayList<>();
		RelationInverse rel = ModelSupport.findRelation(reltype);
		if (rel != null) {
			res
				.add(
					getRelation(
						entityId, otherId, rel.getRelType(), rel.getSubReltype(), rel.getRelClass(), entity));
		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc) {
		return prepareQualifier(
			doc, "//*[local-name() = 'resource']//*[local-name() = 'resourceType']", DNET_DATA_CITE_RESOURCE);
	}

	@Override
	protected List<StructuredProperty> prepareResultPids(final Document doc) {
		final Set<StructuredProperty> res = new HashSet<>();
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc, "//oaf:identifier", "@identifierType", DNET_PID_TYPES));
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc,
					"//*[local-name()='identifier' and ./@identifierType != 'URL' and ./@identifierType != 'landingPage']",
					"@identifierType", DNET_PID_TYPES));
		res
			.addAll(
				prepareListStructPropsWithValidQualifier(
					doc,
					"//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType != 'URL' and ./@alternateIdentifierType != 'landingPage']",
					"@alternateIdentifierType", DNET_PID_TYPES));

		return res
			.stream()
			.map(CleaningFunctions::normalizePidValue)
			.collect(Collectors.toList());
	}

}
