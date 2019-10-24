package eu.dnetlib.dhp.graph;


import com.googlecode.protobuf.format.JsonFormat;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.schema.oaf.*;

import java.io.Serializable;


public class ProtoConverter implements Serializable {


    public static Oaf convert(String s) {
        try {
            final OafProtos.Oaf.Builder builder =  OafProtos.Oaf.newBuilder();
            JsonFormat.merge(s, builder);

            if (builder.getKind() == KindProtos.Kind.entity)
                return convertEntity(builder);
            else {
               return convertRelation(builder);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Relation convertRelation(OafProtos.Oaf.Builder oaf) {
        return new Relation();
    }

    private static OafEntity convertEntity(OafProtos.Oaf.Builder oaf) {

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

    private static Organization convertOrganization(OafProtos.Oaf.Builder oaf) {
        return new Organization();
    }

    private static Datasource convertDataSource(OafProtos.Oaf.Builder oaf) {
        return new Datasource();
    }

    private static Project convertProject(OafProtos.Oaf.Builder oaf) {
        return new Project();
    }

    private static Result convertResult(OafProtos.Oaf.Builder oaf) {
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

    private static Software createSoftware(OafProtos.Oaf.Builder oaf) {
        return new Software();
    }

    private static OtherResearchProducts createORP(OafProtos.Oaf.Builder oaf) {
        return new OtherResearchProducts();
    }

    private static Publication createPublication(OafProtos.Oaf.Builder oaf) {
        return new Publication();
    }

    private static Dataset createDataset(OafProtos.Oaf.Builder oaf) {
        return new Dataset();
    }
}
