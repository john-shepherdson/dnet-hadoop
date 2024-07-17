
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerAuthor;
import eu.dnetlib.broker.objects.OaBrokerExternalReference;
import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerJournal;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProject;
import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;
import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;
import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

public class ConversionUtils {

	private static final Logger log = LoggerFactory.getLogger(ConversionUtils.class);

	private ConversionUtils() {
	}

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

	public static OaBrokerTypedValue oafSubjectToBrokerTypedValue(final Subject sp) {
		return sp != null ? new OaBrokerTypedValue(classId(sp.getQualifier()), sp.getValue()) : null;
	}

	public static OaBrokerRelatedDataset oafDatasetToBrokerDataset(final Dataset d) {
		if (d == null) {
			return null;
		}

		final OaBrokerRelatedDataset res = new OaBrokerRelatedDataset();
		res.setOpenaireId(cleanOpenaireId(d.getId()));
		res.setOriginalId(first(d.getOriginalId()));
		res.setTitle(structPropValue(d.getTitle()));
		res.setPids(allResultPids(d));
		res.setInstances(flatMappedList(d.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res.setCollectedFrom(mappedFirst(d.getCollectedfrom(), KeyValue::getValue));
		return res;
	}

	public static OaBrokerRelatedPublication oafPublicationToBrokerPublication(final Publication p) {
		if (p == null) {
			return null;
		}

		final OaBrokerRelatedPublication res = new OaBrokerRelatedPublication();
		res.setOpenaireId(cleanOpenaireId(p.getId()));
		res.setOriginalId(first(p.getOriginalId()));
		res.setTitle(structPropValue(p.getTitle()));
		res.setPids(allResultPids(p));
		res.setInstances(flatMappedList(p.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res.setCollectedFrom(mappedFirst(p.getCollectedfrom(), KeyValue::getValue));

		return res;
	}

	public static OaBrokerMainEntity oafResultToBrokerResult(final Result result) {
		if (result == null) {
			return null;
		}

		final OaBrokerMainEntity res = new OaBrokerMainEntity();

		res.setOpenaireId(cleanOpenaireId(result.getId()));
		res.setOriginalId(first(result.getOriginalId()));
		res.setTypology(classId(result.getResulttype()));
		res.setTitles(structPropList(result.getTitle()));
		res.setAbstracts(fieldList(result.getDescription()));
		res.setLanguage(classId(result.getLanguage()));
		res.setSubjects(subjectList(result.getSubject()));
		res.setCreators(mappedList(result.getAuthor(), ConversionUtils::oafAuthorToBrokerAuthor));
		res.setPublicationdate(fieldValue(result.getDateofacceptance()));
		res.setPublisher(fieldValue(result.getPublisher()));
		res.setEmbargoenddate(fieldValue(result.getEmbargoenddate()));
		res.setContributor(fieldList(result.getContributor()));
		res
			.setJournal(
				result instanceof Publication ? oafJournalToBrokerJournal(((Publication) result).getJournal()) : null);
		res.setPids(allResultPids(result));
		res.setInstances(flatMappedList(result.getInstance(), ConversionUtils::oafInstanceToBrokerInstances));
		res
			.setExternalReferences(mappedList(result.getExternalReference(), ConversionUtils::oafExtRefToBrokerExtRef));

		return res;
	}

	protected static List<OaBrokerTypedValue> allResultPids(final Result result) {
		final Map<String, StructuredProperty> map = new HashMap<>();

		if (result.getPid() != null) {
			result.getPid().forEach(sp -> map.put(sp.getValue(), sp));
		}

		if (result.getInstance() != null) {
			result.getInstance().forEach(i -> {
				if (i.getPid() != null) {
					i.getPid().forEach(sp -> map.put(sp.getValue(), sp));
				}
				if (i.getAlternateIdentifier() != null) {
					i.getAlternateIdentifier().forEach(sp -> map.put(sp.getValue(), sp));
				}
			});
		}
		return mappedList(map.values(), ConversionUtils::oafPidToBrokerPid);
	}

	public static String cleanOpenaireId(final String id) {
		return id.contains("|") ? StringUtils.substringAfter(id, "|") : id;
	}

	private static OaBrokerAuthor oafAuthorToBrokerAuthor(final Author author) {
		if (author == null) {
			return null;
		}

		final String pids = author.getPid() != null ? author
			.getPid()
			.stream()
			.filter(Objects::nonNull)
			.filter(pid -> pid.getQualifier() != null)
			.filter(pid -> StringUtils.startsWithIgnoreCase(pid.getQualifier().getClassid(), ModelConstants.ORCID))
			.map(StructuredProperty::getValue)
			.map(ConversionUtils::cleanOrcid)
			.filter(StringUtils::isNotBlank)
			.findFirst()
			.orElse(null) : null;

		return new OaBrokerAuthor(author.getFullname(), pids);
	}

	private static String cleanOrcid(final String s) {
		final String match = "//orcid.org/";
		return s.contains(match) ? StringUtils.substringAfter(s, match) : s;
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

	public static OaBrokerProject oafProjectToBrokerProject(final Project p) {
		if (p == null) {
			return null;
		}

		final OaBrokerProject res = new OaBrokerProject();
		res.setOpenaireId(cleanOpenaireId(p.getId()));
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
				log.error("Error in record {}: invalid fundingtree: {}", p.getId(), ftree);
			}
		}

		return res;
	}

	public static OaBrokerRelatedSoftware oafSoftwareToBrokerSoftware(final Software sw) {
		if (sw == null) {
			return null;
		}

		final OaBrokerRelatedSoftware res = new OaBrokerRelatedSoftware();
		res.setOpenaireId(cleanOpenaireId(sw.getId()));
		res.setName(structPropValue(sw.getTitle()));
		res.setDescription(fieldValue(sw.getDescription()));
		res.setRepository(fieldValue(sw.getCodeRepositoryUrl()));
		res.setLandingPage(fieldValue(sw.getDocumentationUrl()));

		return res;
	}

	public static OaBrokerRelatedDatasource oafDatasourceToBrokerDatasource(final Datasource ds) {
		if (ds == null) {
			return null;
		}

		final OaBrokerRelatedDatasource res = new OaBrokerRelatedDatasource();
		res.setName(StringUtils.defaultIfBlank(fieldValue(ds.getOfficialname()), fieldValue(ds.getEnglishname())));
		res.setOpenaireId(cleanOpenaireId(ds.getId()));
		res.setType(classId(ds.getDatasourcetype()));
		return res;
	}

	private static String first(final List<String> list) {
		return list != null && !list.isEmpty() ? list.get(0) : null;
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
			? fl
				.stream()
				.map(Field::getValue)
				.map(s -> StringUtils.abbreviate(s, BrokerConstants.MAX_STRING_SIZE))
				.filter(StringUtils::isNotBlank)
				.limit(BrokerConstants.MAX_LIST_SIZE)
				.collect(Collectors.toList())
			: new ArrayList<>();
	}

	private static List<String> structPropList(final List<StructuredProperty> props) {
		return props != null
			? props
				.stream()
				.map(StructuredProperty::getValue)
				.filter(StringUtils::isNotBlank)
				.limit(BrokerConstants.MAX_LIST_SIZE)
				.collect(Collectors.toList())
			: new ArrayList<>();
	}

	private static List<OaBrokerTypedValue> subjectList(final List<Subject> list) {
		if (list == null) {
			return new ArrayList<>();
		}

		return list
			.stream()
			.map(ConversionUtils::oafSubjectToBrokerTypedValue)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
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

	private static <F, T> List<T> mappedList(final Collection<F> list, final Function<F, T> func) {
		if (list == null) {
			return new ArrayList<>();
		}

		return list
			.stream()
			.map(func::apply)
			.filter(Objects::nonNull)
			.limit(BrokerConstants.MAX_LIST_SIZE)
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
			.limit(BrokerConstants.MAX_LIST_SIZE)
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
