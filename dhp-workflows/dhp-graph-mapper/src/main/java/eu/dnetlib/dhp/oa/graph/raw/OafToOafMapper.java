
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;
import static eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory.*;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import eu.dnetlib.dhp.schema.oaf.utils.ModelHardLimits;

public class OafToOafMapper extends AbstractMdRecordToOafMapper {

	public OafToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId,
		final boolean forceOrginalId) {
		super(vocs, invisible, shouldHashId, forceOrginalId);
	}

	public OafToOafMapper(final VocabularyGroup vocs, final boolean invisible, final boolean shouldHashId) {
		super(vocs, invisible, shouldHashId);
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
				.replace(" ", "")
				.replace("_", "");

			author.setPid(new ArrayList<>());

			if (StringUtils.isNotBlank(pid)) {
				if (type.toLowerCase().startsWith(ORCID)) {
					final String cleanedId = pid
						.replaceAll("http://orcid.org/", "")
						.replaceAll("https://orcid.org/", "");
					author.getPid().add(authorPid(cleanedId, ORCID_PID_TYPE, info));
				} else if (type.startsWith("MAGID")) {
					author.getPid().add(authorPid(pid, MAG_PID_TYPE, info));
				}
			}

			res.add(author);
		}
		return res;
	}

	@Override
	protected Qualifier prepareLanguages(final Document doc) {
		return prepareQualifier(doc, "//dc:language", DNET_LANGUAGES);
	}

	@Override
	protected List<Subject> prepareSubjects(final Document doc, final DataInfo info) {
		return prepareSubjectList(doc, "//dc:subject", info);
	}

	@Override
	protected List<StructuredProperty> prepareTitles(final Document doc) {
		return prepareListStructProps(doc, "//dc:title", MAIN_TITLE_QUALIFIER);
	}

	@Override
	protected List<String> prepareDescriptions(final Document doc) {
		return prepareListFields(doc, "//dc:description")
			.stream()
			.map(d -> StringUtils.left(d, ModelHardLimits.MAX_ABSTRACT_LENGTH))
			.collect(Collectors.toList());
	}

	@Override
	protected Publisher preparePublisher(final Document doc) {
		return publisher(doc.valueOf("//dc:publisher"));
	}

	@Override
	protected List<String> prepareFormats(final Document doc) {
		return prepareListFields(doc, "//dc:format");
	}

	@Override
	protected List<String> prepareContributors(final Document doc) {
		return prepareListFields(doc, "//dc:contributor");
	}

	@Override
	protected List<String> prepareCoverages(final Document doc) {
		return prepareListFields(doc, "//dc:coverage");
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

		final Set<StructuredProperty> pids = new HashSet<>(pid);

		instance
			.setAlternateIdentifier(
				alternateIdentifier.stream().filter(i -> !pids.contains(i)).collect(Collectors.toList()));
		instance.setPid(pid);

		instance.setDateofacceptance(doc.valueOf("//oaf:dateAccepted"));
		instance.setDistributionlocation(doc.valueOf("//oaf:distributionlocation"));
		instance
			.setAccessright(prepareAccessRight(doc, "//oaf:accessrights", DNET_ACCESS_MODES));
		instance.setLicense(license(doc.valueOf("//oaf:license")));
		instance.setRefereed(prepareQualifier(doc, "//oaf:refereed", DNET_REVIEW_LEVELS));
		instance
			.setProcessingchargeamount(doc.valueOf("//oaf:processingchargeamount"));
		instance
			.setProcessingchargecurrency(doc.valueOf("//oaf:processingchargeamount/@currency"));

		final List<Node> nodes = Lists.newArrayList(doc.selectNodes("//dc:identifier"));
		final List<String> url = nodes
			.stream()
			.filter(n -> StringUtils.isNotBlank(n.getText()))
			.map(n -> n.getText().trim())
			.filter(u -> u.startsWith("http"))
			.map(s -> {
				try {
					return URLDecoder.decode(s, "UTF-8");
				} catch (Throwable t) {
					return s;
				}
			})
			.distinct()
			.collect(Collectors.toCollection(ArrayList::new));
		final Set<String> validUrl = validateUrl(url);
		if (!validUrl.isEmpty()) {
			instance.setUrl(new ArrayList<>());
			instance.getUrl().addAll(validUrl);
		}

		return Lists.newArrayList(instance);
	}

	@Override
	protected List<String> prepareSources(final Document doc) {
		return prepareListFields(doc, "//dc:source");
	}

	@Override
	protected List<StructuredProperty> prepareRelevantDates(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	// SOFTWARES

	@Override
	protected Qualifier prepareSoftwareProgrammingLanguage(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareSoftwareCodeRepositoryUrl(
		final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected List<String> prepareSoftwareDocumentationUrls(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	// DATASETS
	@Override
	protected List<GeoLocation> prepareDatasetGeoLocations(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetMetadataVersionNumber(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetLastMetadataUpdate(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetVersion(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetSize(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetDevice(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected String prepareDatasetStorageDate(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	// OTHER PRODUCTS

	@Override
	protected List<String> prepareOtherResearchProductTools(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<String> prepareOtherResearchProductContactGroups(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<String> prepareOtherResearchProductContactPersons(final Document doc) {
		return new ArrayList<>(); // NOT PRESENT IN OAF
	}

	@Override
	protected List<Oaf> addOtherResultRels(
		final Document doc,
		final Entity entity) {

		final String docId = entity.getId();
		final List<Oaf> res = new ArrayList<>();

		for (final Object o : doc.selectNodes("//*[local-name()='relatedDataset']")) {

			final String originalId = ((Node) o).getText();

			if (StringUtils.isNotBlank(originalId)) {
				final String otherId = createOpenaireId(50, originalId, false);
				res
					.add(
						getRelation(
							docId, otherId, RESULT_RESULT, RELATIONSHIP, IS_RELATED_TO, entity));
			}
		}
		return res;
	}

	@Override
	protected Qualifier prepareResourceType(final Document doc) {
		return null; // NOT PRESENT IN OAF
	}

	@Override
	protected List<StructuredProperty> prepareResultPids(final Document doc) {
		return prepareListStructPropsWithValidQualifier(
			doc, "//oaf:identifier", "@identifierType", DNET_PID_TYPES)
				.stream()
				.map(CleaningFunctions::normalizePidValue)
				.collect(Collectors.toList());
	}

}
