
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ConversionUtils {

	public static List<Instance> oafInstanceToBrokerInstances(final eu.dnetlib.dhp.schema.oaf.Instance i) {
		return i.getUrl().stream().map(url -> {
			final Instance r = new Instance();
			r.setUrl(url);
			r.setInstancetype(i.getInstancetype().getClassid());
			r.setLicense(BrokerConstants.OPEN_ACCESS);
			r.setHostedby(i.getHostedby().getValue());
			return r;
		}).collect(Collectors.toList());
	}

	public static Pid oafPidToBrokerPid(final StructuredProperty sp) {
		final Pid pid = new Pid();
		pid.setValue(sp.getValue());
		pid.setType(sp.getQualifier().getClassid());
		return pid;
	}

	public static final Pair<String, String> oafSubjectToPair(final StructuredProperty sp) {
		return Pair.of(sp.getQualifier().getClassid(), sp.getValue());
	}

	public static final eu.dnetlib.broker.objects.Dataset oafDatasetToBrokerDataset(final Dataset d) {
		final eu.dnetlib.broker.objects.Dataset res = new eu.dnetlib.broker.objects.Dataset();
		res.setOriginalId(d.getOriginalId().get(0));
		res.setTitles(structPropList(d.getTitle()));
		res.setPids(d.getPid().stream().map(ConversionUtils::structeredPropertyToPid).collect(Collectors.toList()));
		res.setInstances(d.getInstance().stream().map(ConversionUtils::oafInstanceToBrokerInstances).flatMap(List::stream).collect(Collectors.toList()));
		res.setCollectedFrom(d.getCollectedfrom().stream().map(KeyValue::getValue).collect(Collectors.toList()));
		return res;
	}

	public static final eu.dnetlib.broker.objects.Publication oafPublicationToBrokerPublication(final Publication d) {
		final eu.dnetlib.broker.objects.Publication res = new eu.dnetlib.broker.objects.Publication();
		// TODO This should be reusable
		return res;
	}

	public static final eu.dnetlib.broker.objects.Project oafProjectToBrokerProject(final Project p) {
		final eu.dnetlib.broker.objects.Project res = new eu.dnetlib.broker.objects.Project();
		res.setTitle(fieldValue(p.getTitle()));
		res.setAcronym(fieldValue(p.getAcronym()));
		res.setCode(fieldValue(p.getCode()));
		res.setFunder(null); // TODO
		res.setFundingProgram(null); // TODO
		res.setJurisdiction(null); // TODO
		return res;
	}

	public static final eu.dnetlib.broker.objects.Software oafSoftwareToBrokerSoftware(final Software sw) {
		final eu.dnetlib.broker.objects.Software res = new eu.dnetlib.broker.objects.Software();
		res.setName(structPropValue(sw.getTitle()));
		res.setDescription(fieldValue(sw.getDescription()));
		res.setRepository(fieldValue(sw.getCodeRepositoryUrl()));
		res.setLandingPage(fieldValue(sw.getDocumentationUrl()));
		return res;
	}

	private static Pid structeredPropertyToPid(final StructuredProperty sp) {
		final Pid pid = new Pid();
		pid.setValue(sp.getValue());
		pid.setType(sp.getQualifier().getClassid());
		return pid;
	}

	private static String fieldValue(final Field<String> f) {
		return f != null ? f.getValue() : null;
	}

	private static String fieldValue(final List<Field<String>> fl) {
		return fl != null && !fl.isEmpty() && fl.get(0) != null ? fl.get(0).getValue() : null;
	}

	private static String structPropValue(final List<StructuredProperty> props) {
		return props != null && !props.isEmpty() && props.get(0) != null ? props.get(0).getValue() : null;
	}

	private static List<String> structPropList(final List<StructuredProperty> props) {
		return props != null ? props.stream().map(StructuredProperty::getValue).collect(Collectors.toList()) : new ArrayList<>();
	}
}
