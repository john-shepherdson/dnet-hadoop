
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.field;
import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.structuredProperty;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_ACCESS_MODES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_LANGUAGES;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PUBLICATION_RESOURCE;
import static eu.dnetlib.dhp.schema.common.ModelConstants.IS_RELATED_TO;
import static eu.dnetlib.dhp.schema.common.ModelConstants.PUBLICATION_DATASET;
import static eu.dnetlib.dhp.schema.common.ModelConstants.RESULT_RESULT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.graph.raw.common.PacePerson;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class OafToOafMapper extends AbstractMdRecordToOafMapper {

	public OafToOafMapper(final Map<String, String> code2name) {
		super(code2name);
	}

	@Override
	protected List<Author> prepareAuthors(final Document doc, final DataInfo info) {
		final List<Author> res = new ArrayList<>();
		int pos = 1;
		for (final Object o : doc.selectNodes("//dc:creator")) {
			final Element e = (Element) o;
			final Author author = new Author();
			author.setFullname(e.getText());
			author.setRank(pos++);
			final PacePerson p = new PacePerson(e.getText(), false);
			if (p.isAccurate()) {
				author.setName(p.getNormalisedFirstName());
				author.setSurname(p.getNormalisedSurname());
			}

			final String pid = e.valueOf("./@nameIdentifier");
			final String type = e
				.valueOf("./@nameIdentifierScheme")
				.trim()
				.toUpperCase()
				.replaceAll(" ", "")
				.replaceAll("_", "");

			author.setPid(new ArrayList<>());

			if (StringUtils.isNotBlank(pid)) {
				if (type.startsWith("ORCID")) {
					final String cleanedId = pid
						.replaceAll("http://orcid.org/", "")
						.replaceAll("https://orcid.org/", "");
					author.getPid().add(structuredProperty(cleanedId, ORCID_PID_TYPE, info));
				} else if (type.startsWith("MAGID")) {
					author.getPid().add(structuredProperty(pid, MAG_PID_TYPE, info));
				}
			}

			res.add(author);
		}
		return res;
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//dc:language", DNET_LANGUAGES, DNET_LANGUAGES);
	}

	@Override
	protected List<StructuredProperty> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:subject", info);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//dc:title", MAIN_TITLE_QUALIFIER, info);
	}

	@Override
	protected List<Field<String>> prepareDescriptions(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:description", info);
	}

	@Override
	protected Field<String> preparePublisher(final Document doc, final DataInfo info) {
		return prepareField(doc, "//dc:publisher", info);
	}

	@Override
	protected List<Field<String>> prepareFormats(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:format", info);
	}

	@Override
	protected List<Field<String>> prepareContributors(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:contributor", info);
	}

	@Override
	protected List<Field<String>> prepareCoverages(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:coverage", info);
	}

	@Override
	protected List<Instance> prepareInstances(
		final Document doc,
		final DataInfo info,
		final KeyValue collectedfrom,
		final KeyValue hostedby) {

		final Instance instance = new Instance();
		instance
			.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", DNET_PUBLICATION_RESOURCE, DNET_PUBLICATION_RESOURCE));
		instance.setCollectedfrom(collectedfrom);
		instance.setHostedby(hostedby);
		instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
		instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
		instance
			.setAccessright(prepareQualifier(doc, "//oaf:accessrights", DNET_ACCESS_MODES, DNET_ACCESS_MODES));
		instance.setLicense(field(doc.valueOf("//oaf:license"), info));
		instance.setRefereed(field(doc.valueOf("//oaf:refereed"), info));
		instance
			.setProcessingchargeamount(field(doc.valueOf("//oaf:processingchargeamount"), info));
		instance
			.setProcessingchargecurrency(field(doc.valueOf("//oaf:processingchargeamount/@currency"), info));

		final List<Node> nodes = Lists.newArrayList(doc.selectNodes("//dc:identifier"));
		instance
			.setUrl(nodes
				.stream()
				.filter(n -> StringUtils.isNotBlank(n.getText()))
				.map(n -> n.getText().trim())
				.filter(u -> u.startsWith("http"))
				.distinct()
				.collect(Collectors.toCollection(ArrayList::new)));

		return Lists.newArrayList(instance);
	}

	@Override
	protected List<Field<String>> prepareSources(final Document doc, final DataInfo info) {
		return prepareListFields(doc, "//dc:source", info);
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	// SOFTWARES

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareSoftwareCodeRepositoryUrl(
		final Document doc,
		final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected List<StructuredProperty> prepareSoftwareLicenses(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareSoftwareDocumentationUrls(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	// DATASETS
	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetMetadataVersionNumber(
		final Document doc,
		final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetLastMetadataUpdate(
		final Document doc,
		final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetVersion(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetSize(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetDevice(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetStorageDate(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	// OTHER PRODUCTS

	@Override
	protected List<Field<String>> prepareOtherResearchProductTools(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactGroups(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(
		final Document doc,
		final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Oaf> addOtherResultRels(
		final Document doc,
		final KeyValue collectedFrom,
		final DataInfo info,
		final long lastUpdateTimestamp) {
		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false);

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//*[local-name()='relatedDataset']")) {

			final String originalId = ((Node) o).getText();

			if (StringUtils.isNotBlank(originalId)) {

				final String otherId = createOpenaireId(50, originalId, false);

				res
					.add(getRelation(docId, otherId, RESULT_RESULT, PUBLICATION_DATASET, IS_RELATED_TO, collectedFrom, info, lastUpdateTimestamp));
				res
					.add(getRelation(otherId, docId, RESULT_RESULT, PUBLICATION_DATASET, IS_RELATED_TO, collectedFrom, info, lastUpdateTimestamp));
			}
		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected List<StructuredProperty> prepareResultPids(final Document doc, final DataInfo info) {
		return prepareListStructProps(doc, "//oaf:identifier", "@identifierType", "dnet:pid_types", "dnet:pid_types", info);
	}
}
