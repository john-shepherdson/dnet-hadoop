package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.oa.graph.dump.Constants;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Process implements Serializable {

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
            ri.setZenodo_community(Constants.ZENODO_COMMUNITY_PREFIX + ci.getZenodocommunity());
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
                        Relation direct = new Relation();
                        Relation inverse = new Relation();
                        String nodeType = ModelSupport.idPrefixEntity.get(ds.substring(0, 2));
                        direct.setSource(Node.newInstance(Utils.getContextId(ci.getId()), "context"));
                        direct.setTarget(Node.newInstance(ds, nodeType));
                        direct.setReltype(RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP));
                        direct.setProvenance(Provenance.newInstance("Harvested", "09"));
                        relationList.add(direct);

                        inverse.setTarget(Node.newInstance(Utils.getContextId(ci.getId()), "context"));
                        inverse.setSource(Node.newInstance(ds, nodeType));
                        inverse.setReltype(RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP));
                        inverse.setProvenance(Provenance.newInstance("Harvested", "09"));
                        relationList.add(inverse);

                    });

            return relationList;

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
