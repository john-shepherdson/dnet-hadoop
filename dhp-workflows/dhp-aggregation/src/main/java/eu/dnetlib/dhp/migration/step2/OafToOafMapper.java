package eu.dnetlib.dhp.migration.step2;

import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.createOpenaireId;
import static eu.dnetlib.dhp.migration.utils.OafMapperUtils.field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Node;

import eu.dnetlib.dhp.migration.utils.PacePerson;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
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
			final Node n = (Node) o;
			final Author author = new Author();
			author.setFullname(n.getText());
			author.setRank(pos++);
			final PacePerson p = new PacePerson(n.getText(), false);
			if (p.isAccurate()) {
				author.setName(p.getNormalisedFirstName());
				author.setSurname(p.getNormalisedSurname());
			}
			res.add(author);
		}
		return res;
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//dc:language", "dnet:languages", "dnet:languages");
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
	protected List<Instance> prepareInstances(final Document doc, final DataInfo info, final KeyValue collectedfrom, final KeyValue hostedby) {
		final List<Instance> res = new ArrayList<>();
		for (final Object o : doc.selectNodes("//dc:identifier")) {
			final String url = ((Node) o).getText().trim();
			if (url.startsWith("http")) {
				final Instance instance = new Instance();
				instance.setUrl(Arrays.asList(url));
				instance.setInstancetype(prepareQualifier(doc, "//dr:CobjCategory", "dnet:publication_resource", "dnet:publication_resource"));
				instance.setCollectedfrom(collectedfrom);
				instance.setHostedby(hostedby);
				instance.setDateofacceptance(field(doc.valueOf("//oaf:dateAccepted"), info));
				instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
				instance.setAccessright(prepareQualifier(doc, "//oaf:accessrights", "dnet:access_modes", "dnet:access_modes"));
				instance.setLicense(field(doc.valueOf("//oaf:license"), info));
				instance.setRefereed(field(doc.valueOf("//oaf:refereed"), info));
				instance.setProcessingchargeamount(field(doc.valueOf("//oaf:processingchargeamount"), info));
				instance.setProcessingchargecurrency(field(doc.valueOf("//oaf:processingchargeamount/@currency"), info));
				res.add(instance);
			}
		}
		return res;
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
	protected Field<String> prepareSoftwareCodeRepositoryUrl(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected List<StructuredProperty> prepareSoftwareLicenses(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareSoftwareDocumentationUrls(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	// DATASETS
	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetMetadataVersionNumber(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected Field<String> prepareDatasetLastMetadataUpdate(final Document doc, final DataInfo info) {
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
	protected List<Field<String>> prepareOtherResearchProductTools(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactGroups(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Field<String>> prepareOtherResearchProductContactPersons(final Document doc, final DataInfo info) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Oaf> addOtherResultRels(final Document doc,
			final KeyValue collectedFrom,
			final DataInfo info,
			final long lastUpdateTimestamp) {
		final String docId = createOpenaireId(50, doc.valueOf("//dri:objIdentifier"), false);

		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//*[local-name()='relatedDataset']")) {
			final String otherId = createOpenaireId(50, ((Node) o).getText(), false);

			final Relation r1 = new Relation();
			r1.setRelType("resultResult");
			r1.setSubRelType("publicationDataset");
			r1.setRelClass("isRelatedTo");
			r1.setSource(docId);
			r1.setTarget(otherId);
			r1.setCollectedFrom(Arrays.asList(collectedFrom));
			r1.setDataInfo(info);
			r1.setLastupdatetimestamp(lastUpdateTimestamp);
			res.add(r1);

			final Relation r2 = new Relation();
			r2.setRelType("resultResult");
			r2.setSubRelType("publicationDataset");
			r2.setRelClass("isRelatedTo");
			r2.setSource(otherId);
			r2.setTarget(docId);
			r2.setCollectedFrom(Arrays.asList(collectedFrom));
			r2.setDataInfo(info);
			r2.setLastupdatetimestamp(lastUpdateTimestamp);
			res.add(r2);
		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc, final DataInfo info) {
		return null; // NOT PRESENT IN OAF
	}

}
