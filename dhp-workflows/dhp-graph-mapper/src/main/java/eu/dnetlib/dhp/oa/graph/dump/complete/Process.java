
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;

/**
 * It process the ContextInfo information to produce a new Context Entity or a set of Relations between the generic
 * context entity and datasource/projects related to the context.
 */
public class Process implements Serializable {

	@SuppressWarnings("unchecked")
	public static <R extends ResearchInitiative> R getEntity(ContextInfo ci) {
		try {
			ResearchInitiative ri;
			if (ci.getType().equals("community")) {
				ri = new ResearchCommunity();
				((ResearchCommunity) ri).setSubject(ci.getSubject());
				ri.setType(Constants.RESEARCH_COMMUNITY);
			} else {
				ri = new ResearchInitiative();
				ri.setType(Constants.RESEARCH_INFRASTRUCTURE);
			}
			ri.setId(Utils.getContextId(ci.getId()));
			ri.setOriginalId(ci.getId());

			ri.setDescription(ci.getDescription());
			ri.setName(ci.getName());
			if (StringUtils.isNotEmpty(ci.getZenodocommunity())) {
				ri.setZenodo_community(Constants.ZENODO_COMMUNITY_PREFIX + ci.getZenodocommunity());
			}
			return (R) ri;

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List<Relation> getRelation(ContextInfo ci) {
		try {

			List<Relation> relationList = new ArrayList<>();
			ci
				.getDatasourceList()
				.forEach(ds -> {

					String nodeType = ModelSupport.idPrefixEntity.get(ds.substring(0, 2));

					String contextId = Utils.getContextId(ci.getId());
					relationList
						.add(
							Relation
								.newInstance(
									Node
										.newInstance(
											contextId, eu.dnetlib.dhp.schema.dump.oaf.graph.Constants.CONTEXT_ENTITY),
									Node.newInstance(ds, nodeType),
									RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP),
									Provenance
										.newInstance(
											Constants.USER_CLAIM,
											Constants.DEFAULT_TRUST)));

					relationList
						.add(
							Relation
								.newInstance(
									Node.newInstance(ds, nodeType),
									Node
										.newInstance(
											contextId, eu.dnetlib.dhp.schema.dump.oaf.graph.Constants.CONTEXT_ENTITY),
									RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP),
									Provenance
										.newInstance(
											Constants.USER_CLAIM,
											Constants.DEFAULT_TRUST)));

				});

			return relationList;

		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

}
