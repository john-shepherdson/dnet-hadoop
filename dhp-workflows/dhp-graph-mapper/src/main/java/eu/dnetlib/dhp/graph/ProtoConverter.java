package eu.dnetlib.dhp.graph;

import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.schema.oaf.*;

import java.io.Serializable;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.graph.ProtoUtils.mapKV;

public class ProtoConverter implements Serializable {

    public static Oaf convert(String s) {
        try {
            OafProtos.Oaf oaf = ProtoUtils.parse(s);

            if (oaf.getKind() == KindProtos.Kind.entity)
                return convertEntity(oaf);
            else {
               return convertRelation(oaf);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Relation convertRelation(OafProtos.Oaf oaf) {
        final Relation rel = new Relation();
        final OafProtos.OafRel r = oaf.getRel();
        return rel
                .setSource(r.getSource())
                .setTarget(r.getTarget())
                .setRelType(r.getRelType().toString())
                .setSubRelType(r.getSubRelType().toString())
                .setRelClass(r.getRelClass())
                .setCollectedFrom(r.getCollectedfromList().stream()
                        .map(kv -> mapKV(kv))
                        .collect(Collectors.toList()));
    }

    private static OafEntity convertEntity(OafProtos.Oaf oaf) {

        switch (oaf.getEntity().getType()) {
            case result:
                return convertResult(oaf);
            case project:
                return convertProject(oaf);
            case datasource:
                return convertDataSource(oaf);
            case organization:
                return convertOrganization(oaf);
            default:
                throw new RuntimeException("received unknown type");
        }
    }

    private static Organization convertOrganization(OafProtos.Oaf oaf) {
        return new Organization();
    }

    private static Datasource convertDataSource(OafProtos.Oaf oaf) {
        return new Datasource();
    }

    private static Project convertProject(OafProtos.Oaf oaf) {
        return new Project();
    }

    private static Result convertResult(OafProtos.Oaf oaf) {
        switch (oaf.getEntity().getResult().getMetadata().getResulttype().getClassid()) {
            case "dataset":
                return createDataset(oaf);
            case "publication":
                return createPublication(oaf);
            case "software":
                return createSoftware(oaf);
            case "orp":
                return createORP(oaf);
            default:
                throw new RuntimeException("received unknown type :"+oaf.getEntity().getResult().getMetadata().getResulttype().getClassid());
        }
    }

    private static Software createSoftware(OafProtos.Oaf oaf) {
        return new Software();
    }

    private static OtherResearchProducts createORP(OafProtos.Oaf oaf) {
        return new OtherResearchProducts();
    }

    private static Publication createPublication(OafProtos.Oaf oaf) {
        return new Publication();
    }

    private static Dataset createDataset(OafProtos.Oaf oaf) {
        return new Dataset();
    }
}
