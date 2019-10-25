package eu.dnetlib.dhp.graph;

import eu.dnetlib.data.proto.*;
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
        final OrganizationProtos.Organization.Metadata m = oaf.getEntity().getOrganization().getMetadata();
        final Organization org = setOaf(new Organization(), oaf);
        return setEntity(org, oaf)
                .setLegalshortname(mapStringField(m.getLegalshortname()))
                .setLegalname(mapStringField(m.getLegalname()))
                .setAlternativeNames(m.getAlternativeNamesList().
                        stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setWebsiteurl(mapStringField(m.getWebsiteurl()))
                .setLogourl(mapStringField(m.getLogourl()))
                .setEclegalbody(mapStringField(m.getEclegalbody()))
                .setEclegalperson(mapStringField(m.getEclegalperson()))
                .setEcnonprofit(mapStringField(m.getEcnonprofit()))
                .setEcresearchorganization(mapStringField(m.getEcresearchorganization()))
                .setEchighereducation(mapStringField(m.getEchighereducation()))
                .setEcinternationalorganizationeurinterests(mapStringField(m.getEcinternationalorganizationeurinterests()))
                .setEcinternationalorganization(mapStringField(m.getEcinternationalorganization()))
                .setEcenterprise(mapStringField(m.getEcenterprise()))
                .setEcsmevalidated(mapStringField(m.getEcsmevalidated()))
                .setEcnutscode(mapStringField(m.getEcnutscode()))
                .setCountry(mapQualifier(m.getCountry()));
   }

    private static Datasource convertDataSource(OafProtos.Oaf oaf) {
        final DatasourceProtos.Datasource.Metadata m = oaf.getEntity().getDatasource().getMetadata();
        final Datasource datasource = setOaf(new Datasource(), oaf);
        return setEntity(datasource, oaf)
                .setAccessinfopackage(m.getAccessinfopackageList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setCertificates(mapStringField(m.getCertificates()))
                .setCitationguidelineurl(mapStringField(m.getCitationguidelineurl()))
                .setContactemail(mapStringField(m.getContactemail()))
                .setDatabaseaccessrestriction(mapStringField(m.getDatabaseaccessrestriction()))
                .setDatabaseaccesstype(mapStringField(m.getDatabaseaccesstype()))
                .setDataprovider(mapBoolField(m.getDataprovider()))
                .setDatasourcetype(mapQualifier(m.getDatasourcetype()))
                .setDatauploadrestriction(mapStringField(m.getDatauploadrestriction()))
                .setCitationguidelineurl(mapStringField(m.getCitationguidelineurl()))
                .setDatauploadtype(mapStringField(m.getDatauploadtype()))
                .setDateofvalidation(mapStringField(m.getDateofvalidation()))
                .setDescription(mapStringField(m.getDescription()))
                .setEnglishname(mapStringField(m.getEnglishname()))
                .setLatitude(mapStringField(m.getLatitude()))
                .setLongitude(mapStringField(m.getLongitude()))
                .setLogourl(mapStringField(m.getLogourl()))
                .setMissionstatementurl(mapStringField(m.getMissionstatementurl()))
                .setNamespaceprefix(mapStringField(m.getNamespaceprefix()))
                .setOdcontenttypes(m.getOdcontenttypesList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setOdlanguages(m.getOdlanguagesList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setOdnumberofitems(mapStringField(m.getOdnumberofitems()))
                .setOdnumberofitemsdate(mapStringField(m.getOdnumberofitemsdate()))
                .setOdpolicies(mapStringField(m.getOdpolicies()))
                .setOfficialname(mapStringField(m.getOfficialname()))
                .setOpenairecompatibility(mapQualifier(m.getOpenairecompatibility()))
                .setPidsystems(mapStringField(m.getPidsystems()))
                .setPolicies(m.getPoliciesList()
                        .stream()
                        .map(ProtoUtils::mapKV)
                        .collect(Collectors.toList()))
                .setQualitymanagementkind(mapStringField(m.getQualitymanagementkind()))
                .setReleaseenddate(mapStringField(m.getReleaseenddate()))
                .setServiceprovider(mapBoolField(m.getServiceprovider()))
                .setReleasestartdate(mapStringField(m.getReleasestartdate()))
                .setSubjects(m.getSubjectsList()
                        .stream()
                        .map(ProtoUtils::mapStructuredProperty)
                        .collect(Collectors.toList()))
                .setVersioning(mapBoolField(m.getVersioning()))
                .setWebsiteurl(mapStringField(m.getWebsiteurl()))
                .setJournal(mapJournal(m.getJournal()));
    }

    private static Project convertProject(OafProtos.Oaf oaf) {
        final ProjectProtos.Project.Metadata m = oaf.getEntity().getProject().getMetadata();
        final Project project = setOaf(new Project(), oaf);
        return setEntity(project, oaf)
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
                .setSubjects(m.getSubjectsList().stream()
                        .map(sp -> mapStructuredProperty(sp))
                        .collect(Collectors.toList()))
                .setTitle(mapStringField(m.getTitle()))
                .setWebsiteurl(mapStringField(m.getWebsiteurl()))
                .setFundingtree(m.getFundingtreeList().stream()
                        .map(f -> mapStringField(f))
                        .collect(Collectors.toList()))
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
                throw new RuntimeException("received unknown type :" + oaf.getEntity().getResult().getMetadata().getResulttype().getClassid());
        }
    }

    private static Software createSoftware(OafProtos.Oaf oaf) {
        final ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        final Software software = setOaf(new Software(), oaf);
        




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
