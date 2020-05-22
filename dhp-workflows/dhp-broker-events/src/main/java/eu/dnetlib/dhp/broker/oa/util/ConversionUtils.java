
package eu.dnetlib.dhp.broker.oa.util;

import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ConversionUtils {

	public static Stream<Instance> oafInstanceToBrokerInstances(final eu.dnetlib.dhp.schema.oaf.Instance i) {
		return i.getUrl().stream().map(url -> {
			final Instance r = new Instance();
			r.setUrl(url);
			r.setInstancetype(i.getInstancetype().getClassid());
			r.setLicense(BrokerConstants.OPEN_ACCESS);
			r.setHostedby(i.getHostedby().getValue());
			return r;
		});
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
		// TODO
		return res;
	}

	public static final eu.dnetlib.broker.objects.Publication oafPublicationToBrokerPublication(final Publication d) {
		final eu.dnetlib.broker.objects.Publication res = new eu.dnetlib.broker.objects.Publication();
		// TODO
		return res;
	}
}
