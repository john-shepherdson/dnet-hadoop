
package eu.dnetlib.dhp.sx.graph.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;

import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser.Node;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.scholexplorer.DLIDataset;
import eu.dnetlib.dhp.schema.scholexplorer.ProvenaceInfo;
import eu.dnetlib.scholexplorer.relation.RelationMapper;

public class DatasetScholexplorerParser extends AbstractScholexplorerParser {
	@Override
	public List<Oaf> parseObject(String record, final RelationMapper relationMapper) {
		try {
			final DLIDataset parsedObject = new DLIDataset();
			final VTDGen vg = new VTDGen();
			vg.setDoc(record.getBytes());
			final List<Oaf> result = new ArrayList<>();
			vg.parse(true);

			final VTDNav vn = vg.getNav();
			final AutoPilot ap = new AutoPilot(vn);

			DataInfo di = new DataInfo();
			di.setTrust("0.9");
			di.setDeletedbyinference(false);
			di.setInvisible(false);
			parsedObject.setDataInfo(di);

			parsedObject
				.setOriginalId(
					Collections
						.singletonList(
							VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='recordIdentifier']")));

			parsedObject
				.setOriginalObjIdentifier(
					VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='objIdentifier']"));
			String dateOfCollection = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='dateOfCollection']");
			parsedObject.setDateofcollection(dateOfCollection);

			final String resolvedDate = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='resolvedDate']");

			if (StringUtils.isNotBlank(resolvedDate)) {
				StructuredProperty currentDate = new StructuredProperty();
				currentDate.setValue(resolvedDate);
				final Qualifier dateQualifier = new Qualifier();
				dateQualifier.setClassname("resolvedDate");
				dateQualifier.setClassid("resolvedDate");
				dateQualifier.setSchemename("dnet::date");
				dateQualifier.setSchemeid("dnet::date");
				currentDate.setQualifier(dateQualifier);
				parsedObject.setRelevantdate(Collections.singletonList(currentDate));
			}
			final String completionStatus = VtdUtilityParser
				.getSingleValue(ap, vn, "//*[local-name()='completionStatus']");
			final String provisionMode = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='provisionMode']");

			final String publisher = VtdUtilityParser
				.getSingleValue(
					ap, vn, "//*[local-name()='resource']/*[local-name()='publisher']");

			List<VtdUtilityParser.Node> collectedFromNodes = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap,
					vn,
					"//*[local-name()='collectedFrom']",
					Arrays.asList("name", "id", "mode", "completionStatus"));

			List<VtdUtilityParser.Node> resolvededFromNodes = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap,
					vn,
					"//*[local-name()='resolvedFrom']",
					Arrays.asList("name", "id", "mode", "completionStatus"));

			Field<String> pf = new Field<>();
			pf.setValue(publisher);

			parsedObject.setPublisher(pf);
			final List<ProvenaceInfo> provenances = new ArrayList<>();
			if (collectedFromNodes != null && collectedFromNodes.size() > 0) {
				collectedFromNodes
					.forEach(
						it -> {
							final ProvenaceInfo provenance = new ProvenaceInfo();
							provenance.setId(it.getAttributes().get("id"));
							provenance.setName(it.getAttributes().get("name"));
							provenance.setCollectionMode(provisionMode);
							provenance.setCompletionStatus(it.getAttributes().get("completionStatus"));
							provenances.add(provenance);
						});
			}

			if (resolvededFromNodes != null && resolvededFromNodes.size() > 0) {
				resolvededFromNodes
					.forEach(
						it -> {
							final ProvenaceInfo provenance = new ProvenaceInfo();
							provenance.setId(it.getAttributes().get("id"));
							provenance.setName(it.getAttributes().get("name"));
							provenance.setCollectionMode("resolved");
							provenance.setCompletionStatus(it.getAttributes().get("completionStatus"));
							provenances.add(provenance);
						});
			}

			parsedObject.setDlicollectedfrom(provenances);
			parsedObject
				.setCollectedfrom(
					parsedObject
						.getDlicollectedfrom()
						.stream()
						.map(
							p -> {
								final KeyValue cf = new KeyValue();
								cf.setKey(p.getId());
								cf.setValue(p.getName());
								return cf;
							})
						.collect(Collectors.toList()));
			parsedObject
				.setCompletionStatus(
					VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='completionStatus']"));

			final List<Node> identifierType = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap,
					vn,
					"//*[local-name()='resource']/*[local-name()='identifier']",
					Collections.singletonList("identifierType"));

			StructuredProperty currentPid = extractIdentifier(identifierType, "identifierType");
			if (currentPid == null)
				return null;
			inferPid(currentPid);
			parsedObject.setPid(Collections.singletonList(currentPid));

			String resolvedURL = null;

			switch (currentPid.getQualifier().getClassname().toLowerCase()) {
				case "uniprot":
					resolvedURL = "https://www.uniprot.org/uniprot/" + currentPid.getValue();
					break;
				case "ena":
					if (StringUtils.isNotBlank(currentPid.getValue()) && currentPid.getValue().length() > 7)
						resolvedURL = "https://www.ebi.ac.uk/ena/data/view/" + currentPid.getValue().substring(0, 8);
					break;
				case "chembl":
					resolvedURL = "https://www.ebi.ac.uk/chembl/compound_report_card/" + currentPid.getValue();
					break;

				case "ncbi-n":
					resolvedURL = "https://www.ncbi.nlm.nih.gov/nuccore/" + currentPid.getValue();
					break;
				case "ncbi-p":
					resolvedURL = "https://www.ncbi.nlm.nih.gov/nuccore/" + currentPid.getValue();
					break;
				case "genbank":
					resolvedURL = "https://www.ncbi.nlm.nih.gov/nuccore/" + currentPid.getValue();
					break;
				case "pdb":
					resolvedURL = "https://www.ncbi.nlm.nih.gov/nuccore/" + currentPid.getValue();
					break;
				case "url":
					resolvedURL = currentPid.getValue();
					break;
			}

			final String sourceId = generateId(
				currentPid.getValue(), currentPid.getQualifier().getClassid(), "dataset");
			parsedObject.setId(sourceId);

			List<String> descs = VtdUtilityParser.getTextValue(ap, vn, "//*[local-name()='description']");
			if (descs != null && descs.size() > 0)
				parsedObject
					.setDescription(
						descs
							.stream()
//							.map(it -> it.length() < 10000 ? it : it.substring(0, 10000))
							.map(
								it -> {
									final Field<String> d = new Field<>();
									d.setValue(it);
									return d;
								})
							.collect(Collectors.toList()));

			final List<Node> relatedIdentifiers = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap,
					vn,
					"//*[local-name()='relatedIdentifier']",
					Arrays
						.asList(
							"relatedIdentifierType", "relationType", "entityType", "inverseRelationType"));

			generateRelations(
				relationMapper, parsedObject, result, di, dateOfCollection, relatedIdentifiers);

			final List<Node> hostedBy = VtdUtilityParser
				.getTextValuesWithAttributes(
					ap, vn, "//*[local-name()='hostedBy']", Arrays.asList("id", "name"));

			if (hostedBy != null) {
				parsedObject
					.setInstance(
						hostedBy
							.stream()
							.map(
								it -> {
									final Instance i = new Instance();
									i.setUrl(Collections.singletonList(currentPid.getValue()));
									KeyValue h = new KeyValue();
									i.setHostedby(h);
									h.setKey(it.getAttributes().get("id"));
									h.setValue(it.getAttributes().get("name"));
									return i;
								})
							.collect(Collectors.toList()));
			}

			List<StructuredProperty> subjects = extractSubject(
				VtdUtilityParser
					.getTextValuesWithAttributes(
						ap,
						vn,
						"//*[local-name()='resource']//*[local-name()='subject']",
						Collections.singletonList("subjectScheme")));

			parsedObject.setSubject(subjects);

			Qualifier q = new Qualifier();
			q.setClassname("dataset");
			q.setClassid("dataset");
			q.setSchemename("dataset");
			q.setSchemeid("dataset");
			parsedObject.setResulttype(q);

			parsedObject.setCompletionStatus(completionStatus);

			final List<String> creators = VtdUtilityParser
				.getTextValue(
					ap,
					vn,
					"//*[local-name()='resource']//*[local-name()='creator']/*[local-name()='creatorName']");
			if (creators != null && creators.size() > 0) {
				parsedObject
					.setAuthor(
						creators
							.stream()
							.map(
								a -> {
									final Author author = new Author();
									author.setFullname(a);
									return author;
								})
							.collect(Collectors.toList()));
			}
			final List<String> titles = VtdUtilityParser
				.getTextValue(
					ap, vn, "//*[local-name()='resource']//*[local-name()='title']");
			if (titles != null && titles.size() > 0) {
				parsedObject
					.setTitle(
						titles
							.stream()
							.map(
								t -> {
									final StructuredProperty st = new StructuredProperty();
									st.setValue(t);
									st
										.setQualifier(
											generateQualifier(
												"main title", "main title", "dnet:dataCite_title",
												"dnet:dataCite_title"));
									return st;
								})
							.collect(Collectors.toList()));
			}

			final List<String> dates = VtdUtilityParser
				.getTextValue(
					ap,
					vn,
					"//*[local-name()='resource']/*[local-name()='dates']/*[local-name()='date']");

			if (dates != null && dates.size() > 0) {
				parsedObject
					.setRelevantdate(
						dates
							.stream()
							.map(
								cd -> {
									StructuredProperty date = new StructuredProperty();
									date.setValue(cd);
									final Qualifier dq = new Qualifier();
									dq.setClassname("date");
									dq.setClassid("date");
									dq.setSchemename("dnet::date");
									dq.setSchemeid("dnet::date");
									date.setQualifier(dq);
									return date;
								})
							.collect(Collectors.toList()));
			}

			// TERRIBLE HACK TO AVOID EMPTY COLLECTED FROM
			if (parsedObject.getDlicollectedfrom() == null) {

				final KeyValue cf = new KeyValue();
				cf.setKey("dli_________::europe_pmc__");
				cf.setValue("Europe PMC");
				parsedObject.setCollectedfrom(Collections.singletonList(cf));
			}

			if (StringUtils.isNotBlank(resolvedURL)) {
				Instance i = new Instance();
				i.setCollectedfrom(parsedObject.getCollectedfrom().get(0));
				i.setUrl(Collections.singletonList(resolvedURL));
				parsedObject.setInstance(Collections.singletonList(i));
			}

			result.add(parsedObject);
			return result;
		} catch (Throwable e) {
			log.error("Error on parsing record " + record, e);
			return null;
		}
	}
}
