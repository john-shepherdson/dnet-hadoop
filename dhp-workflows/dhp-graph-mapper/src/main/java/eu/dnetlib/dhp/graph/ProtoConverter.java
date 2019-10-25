package eu.dnetlib.dhp.graph;

import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.schema.oaf.*;

import java.io.Serializable;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.graph.ProtoUtils.mapDataInfo;
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
        final OafProtos.OafRel r = oaf.getRel();
        final Relation rel = new Relation();
        rel.setDataInfo(mapDataInfo(oaf.getDataInfo()));
        rel.setLastupdatetimestamp(oaf.getLastupdatetimestamp());
        return rel
                .setSource(r.getSource())
                .setTarget(r.getTarget())
                .setRelType(r.getRelType().toString())
                .setSubRelType(r.getSubRelType().toString())
                .setRelClass(r.getRelClass())
                .setCollectedFrom(r.getCollectedfromCount() > 0 ?
                        r.getCollectedfromList().stream()
                            .map(kv -> mapKV(kv))
                            .collect(Collectors.toList()) : null);
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
        final Datasource result = new Datasource();


        //Set Oaf Fields
        result.setDataInfo(ProtoUtils.mapDataInfo(oaf.getDataInfo()));

        result.setLastupdatetimestamp(oaf.getLastupdatetimestamp());

        //setting Entity fields
        final OafProtos.OafEntity entity = oaf.getEntity();

        result.setId(entity.getId());

        result.setOriginalId(entity.getOriginalIdList());

        result.setCollectedfrom(entity.getCollectedfromList()
                .stream()
                .map(ProtoUtils::mapKV)
                .collect(Collectors.toList()));

        result.setPid(entity.getPidList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));

        result.setDateofcollection(entity.getDateofcollection());

        result.setDateoftransformation(entity.getDateoftransformation());

        result.setExtraInfo(entity.getExtraInfoList()
                .stream()
                .map(ProtoUtils::mapExtraInfo)
                .collect(Collectors.toList()));

        return result;
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

        Publication result = new Publication();

        //Set Oaf Fields
        result.setDataInfo(ProtoUtils.mapDataInfo(oaf.getDataInfo()));

        result.setLastupdatetimestamp(oaf.getLastupdatetimestamp());

        //setting Entity fields
        final OafProtos.OafEntity entity = oaf.getEntity();

        result.setId(entity.getId());

        result.setJournal(null);

        result.setAuthor(null);

        result.setChildren(null);

        result.setCollectedfrom(entity.getCollectedfromList()
                .stream()
                .map(ProtoUtils::mapKV)
                .collect(Collectors.toList()));

        result.setContext(null);

        result.setContributor(null);

        result.setCountry(null);

        result.setCoverage(null);

        result.setDateofacceptance(result.getDateofacceptance());

        result.setDateofcollection(entity.getDateofcollection());

        result.setDateoftransformation(entity.getDateoftransformation());

        result.setDescription(entity.getResult().getMetadata().getDescriptionList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));

        result.setEmbargoenddate(null);

        result.setExternalReference(null);

        result.setExtraInfo(entity.getExtraInfoList()
                .stream()
                .map(ProtoUtils::mapExtraInfo)
                .collect(Collectors.toList()));

        result.setFormat(entity.getResult().getMetadata().getFormatList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));

        result.setFulltext(null);

        result.setInstance(null);

        result.setLanguage(ProtoUtils.mapQualifier(entity.getResult().getMetadata().getLanguage()));

        result.setOaiprovenance(null);

        result.setOriginalId(entity.getOriginalIdList());

        result.setPid(entity.getPidList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));

        result.setPublisher(ProtoUtils.mapStringField(entity.getResult().getMetadata().getPublisher()));

        result.setRefereed(null);

        result.setRelevantdate(null);

        result.setResourcetype(null);

        result.setResulttype(null);

        result.setSource(entity.getResult().getMetadata().getSourceList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));

        result.setSubject(null);

        result.setTitle(entity.getResult().getMetadata().getTitleList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));

        return result;
    }

    private static Dataset createDataset(OafProtos.Oaf oaf) {
        return new Dataset();
    }
}
