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
            throw new RuntimeException("error on getting " + s, e);
        }
    }

    private static Relation convertRelation(OafProtos.Oaf oaf) {
        final OafProtos.OafRel r = oaf.getRel();
        final Relation rel = new Relation();
        rel.setDataInfo(mapDataInfo(oaf.getDataInfo()));
        rel.setLastupdatetimestamp(oaf.getLastupdatetimestamp());
        rel.setSource(r.getSource());
        rel.setTarget(r.getTarget());
        rel.setRelType(r.getRelType().toString());
        rel.setSubRelType(r.getSubRelType().toString());
        rel.setRelClass(r.getRelClass());
        rel.setCollectedFrom(r.getCollectedfromCount() > 0 ?
                r.getCollectedfromList().stream()
                        .map(kv -> mapKV(kv))
                        .collect(Collectors.toList()) : null);
        return rel;
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
        setEntity(org, oaf);
        org.setLegalshortname(mapStringField(m.getLegalshortname()));
        org.setLegalname(mapStringField(m.getLegalname()));
        org.setAlternativeNames(m.getAlternativeNamesList().
                stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        org.setWebsiteurl(mapStringField(m.getWebsiteurl()));
        org.setLogourl(mapStringField(m.getLogourl()));
        org.setEclegalbody(mapStringField(m.getEclegalbody()));
        org.setEclegalperson(mapStringField(m.getEclegalperson()));
        org.setEcnonprofit(mapStringField(m.getEcnonprofit()));
        org.setEcresearchorganization(mapStringField(m.getEcresearchorganization()));
        org.setEchighereducation(mapStringField(m.getEchighereducation()));
        org.setEcinternationalorganizationeurinterests(mapStringField(m.getEcinternationalorganizationeurinterests()));
        org.setEcinternationalorganization(mapStringField(m.getEcinternationalorganization()));
        org.setEcenterprise(mapStringField(m.getEcenterprise()));
        org.setEcsmevalidated(mapStringField(m.getEcsmevalidated()));
        org.setEcnutscode(mapStringField(m.getEcnutscode()));
        org.setCountry(mapQualifier(m.getCountry()));

        return org;
    }

    private static Datasource convertDataSource(OafProtos.Oaf oaf) {
        final DatasourceProtos.Datasource.Metadata m = oaf.getEntity().getDatasource().getMetadata();
        final Datasource datasource = setOaf(new Datasource(), oaf);
        setEntity(datasource, oaf);
        datasource.setAccessinfopackage(m.getAccessinfopackageList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        datasource.setCertificates(mapStringField(m.getCertificates()));
        datasource.setCitationguidelineurl(mapStringField(m.getCitationguidelineurl()));
        datasource.setContactemail(mapStringField(m.getContactemail()));
        datasource.setDatabaseaccessrestriction(mapStringField(m.getDatabaseaccessrestriction()));
        datasource.setDatabaseaccesstype(mapStringField(m.getDatabaseaccesstype()));
        datasource.setDataprovider(mapBoolField(m.getDataprovider()));
        datasource.setDatasourcetype(mapQualifier(m.getDatasourcetype()));
        datasource.setDatauploadrestriction(mapStringField(m.getDatauploadrestriction()));
        datasource.setCitationguidelineurl(mapStringField(m.getCitationguidelineurl()));
        datasource.setDatauploadtype(mapStringField(m.getDatauploadtype()));
        datasource.setDateofvalidation(mapStringField(m.getDateofvalidation()));
        datasource.setDescription(mapStringField(m.getDescription()));
        datasource.setEnglishname(mapStringField(m.getEnglishname()));
        datasource.setLatitude(mapStringField(m.getLatitude()));
        datasource.setLongitude(mapStringField(m.getLongitude()));
        datasource.setLogourl(mapStringField(m.getLogourl()));
        datasource.setMissionstatementurl(mapStringField(m.getMissionstatementurl()));
        datasource.setNamespaceprefix(mapStringField(m.getNamespaceprefix()));
        datasource.setOdcontenttypes(m.getOdcontenttypesList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        datasource.setOdlanguages(m.getOdlanguagesList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        datasource.setOdnumberofitems(mapStringField(m.getOdnumberofitems()));
        datasource.setOdnumberofitemsdate(mapStringField(m.getOdnumberofitemsdate()));
        datasource.setOdpolicies(mapStringField(m.getOdpolicies()));
        datasource.setOfficialname(mapStringField(m.getOfficialname()));
        datasource.setOpenairecompatibility(mapQualifier(m.getOpenairecompatibility()));
        datasource.setPidsystems(mapStringField(m.getPidsystems()));
        datasource.setPolicies(m.getPoliciesList()
                .stream()
                .map(ProtoUtils::mapKV)
                .collect(Collectors.toList()));
        datasource.setQualitymanagementkind(mapStringField(m.getQualitymanagementkind()));
        datasource.setReleaseenddate(mapStringField(m.getReleaseenddate()));
        datasource.setServiceprovider(mapBoolField(m.getServiceprovider()));
        datasource.setReleasestartdate(mapStringField(m.getReleasestartdate()));
        datasource.setSubjects(m.getSubjectsList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        datasource.setVersioning(mapBoolField(m.getVersioning()));
        datasource.setWebsiteurl(mapStringField(m.getWebsiteurl()));
        datasource.setJournal(mapJournal(m.getJournal()));


        return datasource;
    }

    private static Project convertProject(OafProtos.Oaf oaf) {
        final ProjectProtos.Project.Metadata m = oaf.getEntity().getProject().getMetadata();
        final Project project = setOaf(new Project(), oaf);
        setEntity(project, oaf);
        project.setAcronym(mapStringField(m.getAcronym()));
        project.setCallidentifier(mapStringField(m.getCallidentifier()));
        project.setCode(mapStringField(m.getCode()));
        project.setContactemail(mapStringField(m.getContactemail()));
        project.setContactfax(mapStringField(m.getContactfax()));
        project.setContactfullname(mapStringField(m.getContactfullname()));
        project.setContactphone(mapStringField(m.getContactphone()));
        project.setContracttype(mapQualifier(m.getContracttype()));
        project.setCurrency(mapStringField(m.getCurrency()));
        project.setDuration(mapStringField(m.getDuration()));
        project.setEcarticle29_3(mapStringField(m.getEcarticle293()));
        project.setEcsc39(mapStringField(m.getEcsc39()));
        project.setOamandatepublications(mapStringField(m.getOamandatepublications()));
        project.setStartdate(mapStringField(m.getStartdate()));
        project.setEnddate(mapStringField(m.getEnddate()));
        project.setFundedamount(m.getFundedamount());
        project.setTotalcost(m.getTotalcost());
        project.setKeywords(mapStringField(m.getKeywords()));
        project.setSubjects(m.getSubjectsList().stream()
                .map(sp -> mapStructuredProperty(sp))
                .collect(Collectors.toList()));
        project.setTitle(mapStringField(m.getTitle()));
        project.setWebsiteurl(mapStringField(m.getWebsiteurl()));
        project.setFundingtree(m.getFundingtreeList().stream()
                .map(f -> mapStringField(f))
                .collect(Collectors.toList()));
        project.setJsonextrainfo(mapStringField(m.getJsonextrainfo()));
        project.setSummary(mapStringField(m.getSummary()));
        project.setOptional1(mapStringField(m.getOptional1()));
        project.setOptional2(mapStringField(m.getOptional2()));
        return project;
    }

    private static Result convertResult(OafProtos.Oaf oaf) {
        switch (oaf.getEntity().getResult().getMetadata().getResulttype().getClassid()) {

            case "dataset":
                return createDataset(oaf);
            case "publication":
                return createPublication(oaf);
            case "software":
                return createSoftware(oaf);
            case "other":
                return createORP(oaf);
            default:
                throw new RuntimeException("received unknown type: " + oaf.getEntity().getResult().getMetadata().getResulttype().getClassid());
        }
    }

    private static Software createSoftware(OafProtos.Oaf oaf) {
        ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        Software software = setOaf(new Software(), oaf);
        setEntity(software, oaf);
        setResult(software, oaf);

        software.setDocumentationUrl(m.getDocumentationUrlList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        software.setLicense(m.getLicenseList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        software.setCodeRepositoryUrl(ProtoUtils.mapStringField(m.getCodeRepositoryUrl()));
        software.setProgrammingLanguage(ProtoUtils.mapQualifier(m.getProgrammingLanguage()));
        return software;
    }

    private static OtherResearchProducts createORP(OafProtos.Oaf oaf) {
        ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        OtherResearchProducts otherResearchProducts = setOaf(new OtherResearchProducts(), oaf);
        setEntity(otherResearchProducts, oaf);
        setResult(otherResearchProducts, oaf);
        otherResearchProducts.setContactperson(m.getContactpersonList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        otherResearchProducts.setContactgroup(m.getContactgroupList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        otherResearchProducts.setTool(m.getToolList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));

        return otherResearchProducts;
    }

    private static Publication createPublication(OafProtos.Oaf oaf) {

        ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        Publication publication = setOaf(new Publication(), oaf);
        setEntity(publication, oaf);
        setResult(publication, oaf);
        publication.setJournal(mapJournal(m.getJournal()));
        return publication;
    }

    private static Dataset createDataset(OafProtos.Oaf oaf) {

        ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        Dataset dataset = setOaf(new Dataset(), oaf);
        setEntity(dataset, oaf);
        setResult(dataset, oaf);
        dataset.setStoragedate(ProtoUtils.mapStringField(m.getStoragedate()));
        dataset.setDevice(ProtoUtils.mapStringField(m.getDevice()));
        dataset.setSize(ProtoUtils.mapStringField(m.getSize()));
        dataset.setVersion(ProtoUtils.mapStringField(m.getVersion()));
        dataset.setLastmetadataupdate(ProtoUtils.mapStringField(m.getLastmetadataupdate()));
        dataset.setMetadataversionnumber(ProtoUtils.mapStringField(m.getMetadataversionnumber()));
        dataset.setGeolocation(m.getGeolocationList()
                .stream()
                .map(ProtoUtils::mapGeolocation)
                .collect(Collectors.toList()));
        return dataset;

    }
}
