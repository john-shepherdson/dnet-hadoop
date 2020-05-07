package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingPid extends UpdateInfo<Pid> {

	public static List<EnrichMissingPid> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	private EnrichMissingPid(final Pid highlightValue, final float trust) {
		super("ENRICH/MISSING/PID", highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		payload.getHighlight().getPids().add(getHighlightValue());
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue().getType() + "::" + getHighlightValue().getValue();
	}

}
