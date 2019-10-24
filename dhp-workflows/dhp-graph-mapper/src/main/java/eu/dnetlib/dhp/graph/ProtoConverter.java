package eu.dnetlib.dhp.graph;

import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.ProjectProtos;
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

        //setting oaf field
        //TODO waiting claudio for this method
        //result.setDataInfo(DataInfo.fromOaf(oaf.getDataInfo()));
        result.setLastupdatetimestamp(oaf.getLastupdatetimestamp());

        //setting Entity fields
        result.setId(oaf.getEntity().getId());
        result.setOriginalId(oaf.getEntity().getOriginalIdList());

        //TODO waiting claudio for this method
        result.setCollectedfrom(oaf.getEntity().getCollectedfromList()
                .stream()
                .map(s->new KeyValue())
                .collect(Collectors.toList()));



        return result;
    }

    private static Project convertProject(OafProtos.Oaf oaf) {
        final ProjectProtos.Project.Metadata m = oaf.getEntity().getProject().getMetadata();
        final Project project = new Project();
        project.setDataInfo(mapDataInfo(oaf.getDataInfo()));
        project.setLastupdatetimestamp(oaf.getLastupdatetimestamp());
        return project
                .setAcronym(mapStringField(m.getAcronym()))
                .setCallidentifier(mapStringField(m.getCallidentifier()))
                .setCode(mapStringField(m.getCode()))
                .setContactemail(mapStringField(m.getContactemail()))
                .setContactfax(mapStringField(m.getContactfax()))
                .setContactfullname(mapStringField(m.getContactfullname()))
                .setContactphone(mapStringField(m.getContactphone()))
                .setContracttype(mapQualifier(m.getContracttype()))
                .setCurrency(mapStringField(m.getCurrency()))
                .setDuration(mapStringField(m.getDuration()))
                .setEcarticle29_3(mapStringField(m.getEcarticle293()))
                .setEcsc39(mapStringField(m.getEcsc39()))
                .setOamandatepublications(mapStringField(m.getOamandatepublications()))
                .setStartdate(mapStringField(m.getStartdate()))
                .setEnddate(mapStringField(m.getEnddate()))
                .setFundedamount(m.getFundedamount())
                .setTotalcost(m.getTotalcost())
                .setKeywords(mapStringField(m.getKeywords()))
                .setSubjects(m.getSubjectsCount() > 0 ?
                        m.getSubjectsList().stream()
                            .map(sp -> mapStructuredProperty(sp))
                            .collect(Collectors.toList()) : null)
                .setTitle(mapStringField(m.getTitle()))
                .setWebsiteurl(mapStringField(m.getWebsiteurl()))
                .setFundingtree(m.getFundingtreeCount() > 0 ?
                        m.getFundingtreeList().stream()
                            .map(f -> mapStringField(f))
                            .collect(Collectors.toList()) : null)
                .setJsonextrainfo(mapStringField(m.getJsonextrainfo()))
                .setSummary(mapStringField(m.getSummary()))
                .setOptional1(mapStringField(m.getOptional1()))
                .setOptional2(mapStringField(m.getOptional2()));
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
