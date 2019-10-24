package eu.dnetlib.dhp.graph;

import eu.dnetlib.data.proto.DatasourceProtos;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.schema.oaf.*;

import java.io.Serializable;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.graph.ProtoUtils.*;

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

        result.setOaiprovenance(mapOAIProvenance(entity.getOaiprovenance()));

        //Setting Datasource fields
        final DatasourceProtos.Datasource.Metadata datasource = entity.getDatasource().getMetadata();

        result.setDatasourcetype(mapQualifier(datasource.getDatasourcetype()));
        result.setOpenairecompatibility(mapQualifier(datasource.getOpenairecompatibility()));

        result.setOfficialname(mapStringField(datasource.getOfficialname()));
        result.setEnglishname(mapStringField(datasource.getEnglishname()));
        result.setWebsiteurl(mapStringField(datasource.getWebsiteurl()));
        result.setLogourl(mapStringField(datasource.getLogourl()));
        result.setContactemail(mapStringField(datasource.getContactemail()));
        result.setNamespaceprefix(mapStringField(datasource.getNamespaceprefix()));

        result.setLatitude(mapStringField(datasource.getLatitude()));
        result.setLongitude(mapStringField(datasource.getLongitude()));
        result.setDateofvalidation(mapStringField(datasource.getDateofvalidation()));
        result.setDescription(mapStringField(datasource.getDescription()));

        result.setSubjects(datasource.getSubjectsList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList())
        );

        result.setOdnumberofitems(mapStringField(datasource.getOdnumberofitems()));
        result.setOdnumberofitemsdate(mapStringField(datasource.getOdnumberofitemsdate()));
        result.setOdpolicies(mapStringField(datasource.getOdpolicies()));

        result.setOdlanguages(datasource
                .getOdlanguagesList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList())
        );

        result.setOdcontenttypes(datasource.getOdcontenttypesList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList())
        );

        result.setAccessinfopackage(datasource.getAccessinfopackageList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList())
        );

        //TODO r3data fields

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
        return new Publication();
    }

    private static Dataset createDataset(OafProtos.Oaf oaf) {
        return new Dataset();
    }
}
