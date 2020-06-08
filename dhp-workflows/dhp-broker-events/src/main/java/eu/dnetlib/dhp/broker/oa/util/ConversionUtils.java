
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ConversionUtils {

	private static final Logger log = LoggerFactory.getLogger(ConversionUtils.class);

	public static List<eu.dnetlib.broker.objects.Instance> oafInstanceToBrokerInstances(final Instance i) {
		return i.getUrl().stream().map(url -> {
			return new eu.dnetlib.broker.objects.Instance()
				.setUrl(url)
				.setInstancetype(i.getInstancetype().getClassid())
				.setLicense(BrokerConstants.OPEN_ACCESS)
				.setHostedby(i.getHostedby().getValue());
		}).collect(Collectors.toList());
	}

	public static Pid oafPidToBrokerPid(final StructuredProperty sp) {
		return sp != null ? new Pid()
			.setValue(sp.getValue())
			.setType(sp.getQualifier().getClassid()) : null;
	}

	public static final Pair<String, String> oafSubjectToPair(final StructuredProperty sp) {
		return sp != null ? Pair.of(sp.getQualifier().getClassid(), sp.getValue()) : null;
	}

	public static final eu.dnetlib.broker.objects.Dataset oafDatasetToBrokerDataset(final Dataset d) {
		return d != null ? new eu.dnetlib.broker.objects.Dataset()
			.setOriginalId(d.getOriginalId().get(0))
			.setTitles(structPropList(d.getTitle()))
			.setPids(d.getPid().stream().map(ConversionUtils::oafPidToBrokerPid).collect(Collectors.toList()))
			.setInstances(
				d
					.getInstance()
					.stream()
					.map(ConversionUtils::oafInstanceToBrokerInstances)
					.flatMap(List::stream)
					.collect(Collectors.toList()))
			.setCollectedFrom(d.getCollectedfrom().stream().map(KeyValue::getValue).collect(Collectors.toList()))
			: null;
	}

	public static final eu.dnetlib.broker.objects.Publication oafResultToBrokerPublication(final Result result) {

		return result != null ? new eu.dnetlib.broker.objects.Publication()
			.setOriginalId(result.getOriginalId().get(0))
			.setTitles(structPropList(result.getTitle()))
			.setAbstracts(fieldList(result.getDescription()))
			.setLanguage(result.getLanguage().getClassid())
			.setSubjects(structPropList(result.getSubject()))
			.setCreators(result.getAuthor().stream().map(Author::getFullname).collect(Collectors.toList()))
			.setPublicationdate(result.getDateofcollection())
			.setPublisher(fieldValue(result.getPublisher()))
			.setEmbargoenddate(fieldValue(result.getEmbargoenddate()))
			.setContributor(fieldList(result.getContributor()))
			.setJournal(
				result instanceof Publication ? oafJournalToBrokerJournal(((Publication) result).getJournal()) : null)
			.setCollectedFrom(result.getCollectedfrom().stream().map(KeyValue::getValue).collect(Collectors.toList()))
			.setPids(result.getPid().stream().map(ConversionUtils::oafPidToBrokerPid).collect(Collectors.toList()))
			.setInstances(
				result
					.getInstance()
					.stream()
					.map(ConversionUtils::oafInstanceToBrokerInstances)
					.flatMap(List::stream)
					.collect(Collectors.toList()))
			.setExternalReferences(
				result
					.getExternalReference()
					.stream()
					.map(ConversionUtils::oafExtRefToBrokerExtRef)
					.collect(Collectors.toList()))
			: null;
	}

	private static eu.dnetlib.broker.objects.Journal oafJournalToBrokerJournal(final Journal journal) {
		return journal != null ? new eu.dnetlib.broker.objects.Journal()
			.setName(journal.getName())
			.setIssn(journal.getIssnPrinted())
			.setEissn(journal.getIssnOnline())
			.setLissn(journal.getIssnLinking()) : null;
	}

	private static eu.dnetlib.broker.objects.ExternalReference oafExtRefToBrokerExtRef(final ExternalReference ref) {
		return ref != null ? new eu.dnetlib.broker.objects.ExternalReference()
			.setRefidentifier(ref.getRefidentifier())
			.setSitename(ref.getSitename())
			.setType(ref.getQualifier().getClassid())
			.setUrl(ref.getUrl())
			: null;
	}

	public static final eu.dnetlib.broker.objects.Project oafProjectToBrokerProject(final Project p) {
		if (p == null) {
			return null;
		}

		final eu.dnetlib.broker.objects.Project res = new eu.dnetlib.broker.objects.Project()
			.setTitle(fieldValue(p.getTitle()))
			.setAcronym(fieldValue(p.getAcronym()))
			.setCode(fieldValue(p.getCode()));

		final String ftree = fieldValue(p.getFundingtree());
		if (StringUtils.isNotBlank(ftree)) {
			try {
				final Document fdoc = DocumentHelper.parseText(ftree);
				res.setFunder(fdoc.valueOf("/fundingtree/funder/shortname"));
				res.setJurisdiction(fdoc.valueOf("/fundingtree/funder/jurisdiction"));
				res.setFundingProgram(fdoc.valueOf("//funding_level_0/name"));
			} catch (final DocumentException e) {
				log.error("Error in record " + p.getId() + ": invalid fundingtree: " + ftree);
			}
		}

		return res;
	}

	public static final eu.dnetlib.broker.objects.Software oafSoftwareToBrokerSoftware(final Software sw) {
		return sw != null ? new eu.dnetlib.broker.objects.Software()
			.setName(structPropValue(sw.getTitle()))
			.setDescription(fieldValue(sw.getDescription()))
			.setRepository(fieldValue(sw.getCodeRepositoryUrl()))
			.setLandingPage(fieldValue(sw.getDocumentationUrl()))
			: null;
	}

	private static String fieldValue(final Field<String> f) {
		return f != null ? f.getValue() : null;
	}

	private static String fieldValue(final List<Field<String>> fl) {
		return fl != null ? fl.stream().map(Field::getValue).filter(StringUtils::isNotBlank).findFirst().orElse(null)
			: null;
	}

	private static String structPropValue(final List<StructuredProperty> props) {
		return props != null
			? props.stream().map(StructuredProperty::getValue).filter(StringUtils::isNotBlank).findFirst().orElse(null)
			: null;
	}

	private static List<String> fieldList(final List<Field<String>> fl) {
		return fl != null
			? fl.stream().map(Field::getValue).filter(StringUtils::isNotBlank).collect(Collectors.toList())
			: new ArrayList<>();
	}

	private static List<String> structPropList(final List<StructuredProperty> props) {
		return props != null
			? props
				.stream()
				.map(StructuredProperty::getValue)
				.filter(StringUtils::isNotBlank)
				.collect(Collectors.toList())
			: new ArrayList<>();
	}
}
