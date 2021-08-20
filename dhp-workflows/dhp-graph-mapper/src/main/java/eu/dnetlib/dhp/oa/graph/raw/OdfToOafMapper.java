
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Node;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;

public class OdfToOafMapper extends AbstractMdRecordToOafMapper {

	public static final String HTTP_DX_DOI_PREIFX = "http://dx.doi.org/";

	public OdfToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId,
		final boolean forceOrginalId) {
		super(vocs, invisible, shouldHashId, forceOrginalId);
	}

	public OdfToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId) {
		super(vocs, invisible, shouldHashId);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		return prepareListStructProps(
			doc,
			"//*[local-name()='titles']/*[local-name()='title']|//*[local-name()='resource']/*[local-name()='title']",
			MAIN_TITLE_QUALIFIER, info);
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

				author.setAffiliation(prepareListFields(n, "./*[local-name()='affiliation']", info));
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
	protected List<Instance> prepareInstances(
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

		final Set<StructuredProperty> pids = pid.stream().collect(Collectors.toCollection(HashSet::new));

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
		for (final Object o : doc
			.selectNodes("//*[local-name()='alternateIdentifier' and ./@alternateIdentifierType='DOI']")) {
			url.add(HTTP_DX_DOI_PREIFX + ((Node) o).getText().trim());
		}
		for (final Object o : doc.selectNodes("//*[local-name()='identifier' and ./@identifierType='DOI']")) {
			url.add(HTTP_DX_DOI_PREIFX + ((Node) o).getText().trim());
		}
		if (!url.isEmpty()) {
			instance.setUrl(new ArrayList<>());
			instance.getUrl().addAll(url);
		}
		return Arrays.asList(instance);
	}

	protected String trimAndDecodeUrl(String url){
		try {
			return URLDecoder.decode(url.trim(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return url;
		}
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
		return prepareListFields(doc, "//*[local-name()='description' and ./@descriptionType='Abstract']", info);
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//*[local-name()='subject']", info);
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
		final OafEntity entity) {

		final String docId = entity.getId();

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc
			.selectNodes("//*[local-name()='relatedIdentifier' and ./@relatedIdentifierType='OPENAIRE']")) {

			final String originalId = ((Node) o).getText();

			if (StringUtils.isNotBlank(originalId)) {
				final String otherId = createOpenaireId(50, originalId, false);
				final String type = ((Node) o).valueOf("@relationType");

				if (type.equalsIgnoreCase(IS_SUPPLEMENT_TO)) {
					res
						.add(
							getRelation(
								docId, otherId, RESULT_RESULT, SUPPLEMENT, IS_SUPPLEMENT_TO, entity));
					res
						.add(
							getRelation(
								otherId, docId, RESULT_RESULT, SUPPLEMENT, IS_SUPPLEMENTED_BY, entity));
				} else if (type.equalsIgnoreCase(IS_PART_OF)) {
					res
						.add(
							getRelation(
								docId, otherId, RESULT_RESULT, PART, IS_PART_OF, entity));
					res
						.add(
							getRelation(
								otherId, docId, RESULT_RESULT, PART, HAS_PART, entity));
				} else {
					// TODO catch more semantics
				}
			}
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
		final Set<StructuredProperty> res = new HashSet<>();
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
			.map(CleaningFunctions::normalizePidValue)
			.collect(Collectors.toList());
	}

}
