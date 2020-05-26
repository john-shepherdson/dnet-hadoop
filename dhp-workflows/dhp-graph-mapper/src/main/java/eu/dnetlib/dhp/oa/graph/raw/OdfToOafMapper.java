
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.field;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.structuredProperty;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_ACCESS_MODES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_DATA_CITE_DATE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_DATA_CITE_RESOURCE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_LANGUAGES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PUBLICATION_RESOURCE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.HAS_PARTS;
import static eu.dnetlib.dhp.schema.common.ModelConstants.IS_PART_OF;
import static eu.dnetlib.dhp.schema.common.ModelConstants.IS_SUPPLEMENTED_BY;
import static eu.dnetlib.dhp.schema.common.ModelConstants.IS_SUPPLEMENT_TO;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PART;
import static eu.dnetlib.dhp.schema.common.ModelConstants.RESULT_RESULT;
import static eu.dnetlib.dhp.schema.common.ModelConstants.SUPPLEMENT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Node;

import eu.dnetlib.dhp.common.PacePerson;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class OdfToOafMapper extends AbstractMdRecordToOafMapper {

	public static final String HTTP_DX_DOI_PREIFX = "http://dx.doi.org/";

	public OdfToOafMapper(final Map<String, String> code2name) {
		super(code2name);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//datacite:title", MAIN_TITLE_QUALIFIER, info);
	}

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
		final List<Author> res = new ArrayList<>();
		int pos = 1;
		for (final Object o : doc.selectNodes("//datacite:creator")) {
			final Node n = (Node) o;
			final Author author = new Author();
			final String fullname = n.valueOf("./datacite:creatorName");
			author.setFullname(fullname);

			final PacePerson pp = new PacePerson(fullname, false);
			final String name = n.valueOf("./datacite:givenName");
			if (StringUtils.isBlank(name) & pp.isAccurate()) {
				author.setName(pp.getNormalisedFirstName());
			} else {
				author.setName(name);
			}

			final String surname = n.valueOf("./datacite:familyName");
			if (StringUtils.isBlank(surname) & pp.isAccurate()) {
				author.setSurname(pp.getNormalisedSurname());
			} else {
				author.setSurname(surname);
			}

			if (StringUtils.isBlank(author.getFullname())) {
				author.setFullname(String.format("%s, %s", author.getSurname(), author.getName()));
			}

			author.setAffiliation(prepareListFields(n, "./datacite:affiliation", info));
			author.setPid(preparePids(n, info));
			author.setRank(pos++);
			res.add(author);
		}
		return res;
	}

	private List<StructuredProperty> preparePids(final Node n, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : n.selectNodes("./datacite:nameIdentifier")) {

			final String id = ((Node) o).getText();
			final String type = ((Node) o)
				.valueOf("./@nameIdentifierScheme")
				.trim()
				.toUpperCase()
				.replaceAll(" ", "")
				.replaceAll("_", "");

			if (type.startsWith("ORCID")) {
				final String cleanedId = id.replaceAll("http://orcid.org/", "").replaceAll("https://orcid.org/", "");
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
			.setInstancetype(
				prepareQualifier(doc, "//dr:CobjCategory", DNET_PUBLICATION_RESOURCE, DNET_PUBLICATION_RESOURCE));
		instance.setCollectedfrom(collectedfrom);
		instance.setHostedby(hostedby);
		instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
		instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
		instance
			.setAccessright(prepareQualifier(doc, "//oaf:accessrights", DNET_ACCESS_MODES, DNET_ACCESS_MODES));
		instance.setLicense(field(doc.valueOf("//oaf:license"), info));
		instance.setRefereed(field(doc.valueOf("//oaf:refereed"), info));
		instance.setProcessingchargeamount(field(doc.valueOf("//oaf:processingchargeamount"), info));
		instance
			.setProcessingchargecurrency(field(doc.valueOf("//oaf:processingchargeamount/@currency"), info));

		final Set<String> url = new HashSet<>();
		for (final Object o : doc.selectNodes("//datacite:alternateIdentifier[@alternateIdentifierType='URL']")) {
			url.add(((Node) o).getText().trim());
		}
		for (final Object o : doc.selectNodes("//datacite:identifier[@identifierType='URL']")) {
			url.add(((Node) o).getText().trim());
		}
		for (final Object o : doc.selectNodes("//datacite:alternateIdentifier[@alternateIdentifierType='DOI']")) {
			url.add(HTTP_DX_DOI_PREIFX + ((Node) o).getText().trim());
		}
		for (final Object o : doc.selectNodes("//datacite:identifier[@identifierType='DOI']")) {
			url.add(HTTP_DX_DOI_PREIFX + ((Node) o).getText().trim());
		}
		if (!url.isEmpty()) {
			instance.setUrl(new ArrayList<>());
			instance.getUrl().addAll(url);
		}
		return Arrays.asList(instance);
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // Not present in ODF ???
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc, final DataInfo info) {
		final List<StructuredProperty> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//datacite:date")) {
			final String dateType = ((Node) o).valueOf("@dateType");
			if (StringUtils.isBlank(dateType)
				&& !dateType.equalsIgnoreCase("Accepted")
				&& !dateType.equalsIgnoreCase("Issued")
				&& !dateType.equalsIgnoreCase("Updated")
				&& !dateType.equalsIgnoreCase("Available")) {
				res
					.add(
						structuredProperty(
							((Node) o).getText(), "UNKNOWN", "UNKNOWN", DNET_DATA_CITE_DATE, DNET_DATA_CITE_DATE,
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
		return prepareListFields(doc, "//datacite:contributorName", info);
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//datacite:format", info);
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		return prepareField(doc, "//datacite:publisher", info);
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//datacite:description[@descriptionType='Abstract']", info);
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//datacite:subject", info);
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//datacite:language", DNET_LANGUAGES, DNET_LANGUAGES);
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
			doc, "//datacite:contributor[@contributorType='ContactGroup']/datacite:contributorName", info);
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(
		final Document doc,
		final DataInfo info) {
		return prepareListFields(
			doc, "//datacite:contributor[@contributorType='ContactPerson']/datacite:contributorName", info);
	}

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc, final DataInfo info) {
		return prepareQualifier(doc, "//datacite:format", "dnet:programming_languages", "dnet:programming_languages");
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
			doc, "//datacite:relatedIdentifier[@relatedIdentifierType='URL' and @relationType='IsDocumentedBy']", info);
	}

	// DATASETS

	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		final List<GeoLocation> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//datacite:geoLocation")) {
			final GeoLocation loc = new GeoLocation();
			loc.setBox(((Node) o).valueOf("./datacite:geoLocationBox"));
			loc.setPlace(((Node) o).valueOf("./datacite:geoLocationPlace"));
			loc.setPoint(((Node) o).valueOf("./datacite:geoLocationPoint"));
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
		return prepareField(doc, "//datacite:date[@dateType='Updated']", info);
	}

	@Override
	protected Field<String> prepareDatasetVersion(final Document doc, final DataInfo info) {
		return prepareField(doc, "//datacite:version", info);
	}

	@Override
	protected Field<String> prepareDatasetSize(final Document doc, final DataInfo info) {
		return prepareField(doc, "//datacite:size", info);
	}

	@Override
	protected Field<String> prepareDatasetDevice(final Document doc, final DataInfo info) {
		return null; // Not present in ODF ???
	}

	@Override
	protected Field<String> prepareDatasetStorageDate(final Document doc, final DataInfo info) {
		return prepareField(doc, "//datacite:date[@dateType='Issued']", info);
	}

	@Override
	protected List<Oaf> addOtherResultRels(
		final Document doc,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {

		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false);

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//datacite:relatedIdentifier[@relatedIdentifierType='OPENAIRE']")) {

			final String originalId = ((Node) o).getText();

			if (StringUtils.isNotBlank(originalId)) {
				final String otherId = createOpenaireId(50, originalId, false);
				final String type = ((Node) o).valueOf("@relationType");

				if (type.equalsIgnoreCase("IsSupplementTo")) {
					res
						.add(
							getRelation(
								docId, otherId, RESULT_RESULT, SUPPLEMENT, IS_SUPPLEMENT_TO, collectedFrom, info,
								lastUpdateTimestamp));
					res
						.add(
							getRelation(
								otherId, docId, RESULT_RESULT, SUPPLEMENT, IS_SUPPLEMENTED_BY, collectedFrom, info,
								lastUpdateTimestamp));
				} else if (type.equals("IsPartOf")) {

					res
						.add(
							getRelation(
								docId, otherId, RESULT_RESULT, PART, IS_PART_OF, collectedFrom, info,
								lastUpdateTimestamp));
					res
						.add(
							getRelation(
								otherId, docId, RESULT_RESULT, PART, HAS_PARTS, collectedFrom, info,
								lastUpdateTimestamp));
				} else {
				}
			}
		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc, final DataInfo info) {
		return prepareQualifier(
			doc, "//*[local-name() = 'resource']//*[local-name() = 'resourceType']", DNET_DATA_CITE_RESOURCE,
			DNET_DATA_CITE_RESOURCE);
	}
}
