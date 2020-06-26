
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import eu.dnetlib.broker.objects.OaBrokerAuthor;
import eu.dnetlib.broker.objects.OaBrokerExternalReference;
import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerJournal;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProject;
import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;
import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ConversionUtils {

	private static final Logger log = LoggerFactory.getLogger(ConversionUtils.class);

	public static List<OaBrokerInstance> oafInstanceToBrokerInstances(final Instance i) {
		if (i == null) {
			return new ArrayList<>();
		}

		return mappedList(i.getUrl(), url -> {
			final OaBrokerInstance res = new OaBrokerInstance();
			res.setUrl(url);
			res.setInstancetype(classId(i.getInstancetype()));
			res.setLicense(BrokerConstants.OPEN_ACCESS);
			res.setHostedby(kvValue(i.getHostedby()));
			return res;
		});
	}

	public static OaBrokerTypedValue oafPidToBrokerPid(final StructuredProperty sp) {
		return oafStructPropToBrokerTypedValue(sp);
	}

	public static OaBrokerTypedValue oafStructPropToBrokerTypedValue(final StructuredProperty sp) {
		return sp != null ? new OaBrokerTypedValue(classId(sp.getQualifier()), sp.getValue()) : null;
	}

	public static final OaBrokerRelatedDataset oafDatasetToBrokerDataset(final Dataset d) {
		if (d == null) {
			return null;
		}

		final OaBrokerRelatedDataset res = new OaBrokerRelatedDataset();
		res.setOpenaireId(d.getId());
		res.setOriginalId(first(d.getOriginalId()));
		res.setTitle(structPropValue(d.getTitle()));
		res.setPids(mappedList(d.getPid(), ConversionUtils::oafPidToBrokerPid));
		res.setInstances(flatMappedList(d.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res.setCollectedFrom(mappedFirst(d.getCollectedfrom(), KeyValue::getValue));
		return res;
	}

	public static OaBrokerRelatedPublication oafPublicationToBrokerPublication(final Publication p) {
		if (p == null) {
			return null;
		}

		final OaBrokerRelatedPublication res = new OaBrokerRelatedPublication();
		res.setOpenaireId(p.getId());
		res.setOriginalId(first(p.getOriginalId()));
		res.setTitle(structPropValue(p.getTitle()));
		res.setPids(mappedList(p.getPid(), ConversionUtils::oafPidToBrokerPid));
		res.setInstances(flatMappedList(p.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res.setCollectedFrom(mappedFirst(p.getCollectedfrom(), KeyValue::getValue));

		return res;
	}

	public static final OaBrokerMainEntity oafResultToBrokerResult(final Result result) {
		if (result == null) {
			return null;
		}

		final OaBrokerMainEntity res = new OaBrokerMainEntity();

		res.setOpenaireId(result.getId());
		res.setOriginalId(first(result.getOriginalId()));
		res.setTypology(classId(result.getResulttype()));
		res.setTitles(structPropList(result.getTitle()));
		res.setAbstracts(fieldList(result.getDescription()));
		res.setLanguage(classId(result.getLanguage()));
		res.setSubjects(structPropTypedList(result.getSubject()));
		res.setCreators(mappedList(result.getAuthor(), ConversionUtils::oafAuthorToBrokerAuthor));
		res.setPublicationdate(fieldValue(result.getDateofacceptance()));
		res.setPublisher(fieldValue(result.getPublisher()));
		res.setEmbargoenddate(fieldValue(result.getEmbargoenddate()));
		res.setContributor(fieldList(result.getContributor()));
		res
			.setJournal(
				result instanceof Publication ? oafJournalToBrokerJournal(((Publication) result).getJournal()) : null);
		res.setCollectedFromId(mappedFirst(result.getCollectedfrom(), KeyValue::getKey));
		res.setCollectedFromName(mappedFirst(result.getCollectedfrom(), KeyValue::getValue));
		res.setPids(mappedList(result.getPid(), ConversionUtils::oafPidToBrokerPid));
		res.setInstances(flatMappedList(result.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res.setExternalReferences(mappedList(result.getExternalReference(), ConversionUtils::oafExtRefToBrokerExtRef));

		return res;
	}

	private static OaBrokerAuthor oafAuthorToBrokerAuthor(final Author author) {
		if (author == null) {
			return null;
		}

		final String pids = author.getPid() != null ? author
			.getPid()
			.stream()
			.filter(pid -> pid != null)
			.filter(pid -> pid.getQualifier() != null)
			.filter(pid -> pid.getQualifier().getClassid() != null)
			.filter(pid -> pid.getQualifier().getClassid().equalsIgnoreCase("orcid"))
			.map(pid -> pid.getValue())
			.filter(StringUtils::isNotBlank)
			.findFirst()
			.orElse(null) : null;

		return new OaBrokerAuthor(author.getFullname(), pids);
	}

	private static OaBrokerJournal oafJournalToBrokerJournal(final Journal journal) {
		if (journal == null) {
			return null;
		}

		final OaBrokerJournal res = new OaBrokerJournal();
		res.setName(journal.getName());
		res.setIssn(journal.getIssnPrinted());
		res.setEissn(journal.getIssnOnline());
		res.setLissn(journal.getIssnLinking());

		return res;
	}

	private static OaBrokerExternalReference oafExtRefToBrokerExtRef(final ExternalReference ref) {
		if (ref == null) {
			return null;
		}

		final OaBrokerExternalReference res = new OaBrokerExternalReference();
		res.setRefidentifier(ref.getRefidentifier());
		res.setSitename(ref.getSitename());
		res.setType(classId(ref.getQualifier()));
		res.setUrl(ref.getUrl());
		return res;
	}

	public static final OaBrokerProject oafProjectToBrokerProject(final Project p) {
		if (p == null) {
			return null;
		}

		final OaBrokerProject res = new OaBrokerProject();
		res.setOpenaireId(p.getId());
		res.setTitle(fieldValue(p.getTitle()));
		res.setAcronym(fieldValue(p.getAcronym()));
		res.setCode(fieldValue(p.getCode()));

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

	public static final OaBrokerRelatedSoftware oafSoftwareToBrokerSoftware(final Software sw) {
		if (sw == null) {
			return null;
		}

		final OaBrokerRelatedSoftware res = new OaBrokerRelatedSoftware();
		res.setOpenaireId(sw.getId());
		res.setName(structPropValue(sw.getTitle()));
		res.setDescription(fieldValue(sw.getDescription()));
		res.setRepository(fieldValue(sw.getCodeRepositoryUrl()));
		res.setLandingPage(fieldValue(sw.getDocumentationUrl()));

		return res;
	}

	private static String first(final List<String> list) {
		return list != null && list.size() > 0 ? list.get(0) : null;
	}

	private static String kvValue(final KeyValue kv) {
		return kv != null ? kv.getValue() : null;
	}

	private static String fieldValue(final Field<String> f) {
		return f != null ? f.getValue() : null;
	}

	private static String fieldValue(final List<Field<String>> fl) {
		return fl != null ? fl.stream().map(Field::getValue).filter(StringUtils::isNotBlank).findFirst().orElse(null)
			: null;
	}

	private static String classId(final Qualifier q) {
		return q != null ? q.getClassid() : null;
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

	private static List<OaBrokerTypedValue> structPropTypedList(final List<StructuredProperty> list) {
		if (list == null) {
			return new ArrayList<>();
		}

		return list
			.stream()
			.map(ConversionUtils::oafStructPropToBrokerTypedValue)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}

	private static <F, T> List<T> mappedList(final List<F> list, final Function<F, T> func) {
		if (list == null) {
			return new ArrayList<>();
		}

		return list
			.stream()
			.map(func::apply)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}

	private static <F, T> List<T> flatMappedList(final List<F> list, final Function<F, List<T>> func) {
		if (list == null) {
			return new ArrayList<>();
		}

		return list
			.stream()
			.map(func::apply)
			.flatMap(List::stream)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}

	private static <F, T> T mappedFirst(final List<F> list, final Function<F, T> func) {
		if (list == null) {
			return null;
		}

		return list
			.stream()
			.map(func::apply)
			.filter(Objects::nonNull)
			.findFirst()
			.orElse(null);
	}

}
