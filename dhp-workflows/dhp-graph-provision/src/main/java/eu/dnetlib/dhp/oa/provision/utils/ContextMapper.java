
package eu.dnetlib.dhp.oa.provision.utils;

import java.io.*;
import java.util.HashMap;

import eu.dnetlib.dhp.common.api.context.*;
import eu.dnetlib.dhp.common.rest.DNetRestClient;

public class ContextMapper extends HashMap<String, ContextDef> implements Serializable {

	private static final long serialVersionUID = 2159682308502487305L;

	public static ContextMapper fromAPI(final String baseURL) throws Exception {

		final ContextMapper contextMapper = new ContextMapper();

		for (ContextSummary ctx : DNetRestClient
			.doGET(String.format("%s/contexts", baseURL), ContextSummaryList.class)) {

			contextMapper.put(ctx.getId(), new ContextDef(ctx.getId(), ctx.getLabel(), "context", ctx.getType()));

			for (CategorySummary cat : DNetRestClient
				.doGET(String.format("%s/context/%s?all=true", baseURL, ctx.getId()), CategorySummaryList.class)) {
				contextMapper.put(cat.getId(), new ContextDef(cat.getId(), cat.getLabel(), "category", ""));
				if (cat.isHasConcept()) {
					for (ConceptSummary c : DNetRestClient
						.doGET(
							String.format("%s/context/category/%s?all=true", baseURL, cat.getId()),
							ConceptSummaryList.class)) {
						contextMapper.put(c.getId(), new ContextDef(c.getId(), c.getLabel(), "concept", ""));
						if (c.isHasSubConcept()) {
							for (ConceptSummary cs : c.getConcepts()) {
								contextMapper.put(cs.getId(), new ContextDef(cs.getId(), cs.getLabel(), "concept", ""));
								if (cs.isHasSubConcept()) {
									for (ConceptSummary css : cs.getConcepts()) {
										contextMapper
											.put(
												css.getId(),
												new ContextDef(css.getId(), css.getLabel(), "concept", ""));
									}
								}
							}
						}
					}
				}
			}
		}
		return contextMapper;
	}

}
