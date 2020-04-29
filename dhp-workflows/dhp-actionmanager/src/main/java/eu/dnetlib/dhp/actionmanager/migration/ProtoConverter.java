package eu.dnetlib.dhp.actionmanager.migration;

import static eu.dnetlib.data.proto.KindProtos.Kind.entity;
import static eu.dnetlib.data.proto.KindProtos.Kind.relation;
import static eu.dnetlib.data.proto.TypeProtos.*;
import static eu.dnetlib.data.proto.TypeProtos.Type.*;

import com.google.common.collect.Lists;
import com.googlecode.protobuf.format.JsonFormat;
import eu.dnetlib.data.proto.*;
import eu.dnetlib.dhp.schema.oaf.*;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class ProtoConverter implements Serializable {

  public static final String UNKNOWN = "UNKNOWN";
  public static final String NOT_AVAILABLE = "not available";
  public static final String DNET_ACCESS_MODES = "dnet:access_modes";

  public static Oaf convert(OafProtos.Oaf oaf) {
    try {
      switch (oaf.getKind()) {
        case entity:
          return convertEntity(oaf);
        case relation:
          return convertRelation(oaf);
        default:
          throw new IllegalArgumentException("invalid kind " + oaf.getKind());
      }
    } catch (Throwable e) {
      throw new RuntimeException("error on getting " + JsonFormat.printToString(oaf), e);
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
    rel.setCollectedfrom(
        r.getCollectedfromCount() > 0
            ? r.getCollectedfromList().stream().map(kv -> mapKV(kv)).collect(Collectors.toList())
            : null);
    return rel;
  }

  private static OafEntity convertEntity(OafProtos.Oaf oaf) {

    switch (oaf.getEntity().getType()) {
      case result:
        final Result r = convertResult(oaf);
        r.setInstance(convertInstances(oaf));
        return r;
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

  private static List<Instance> convertInstances(OafProtos.Oaf oaf) {

    final ResultProtos.Result r = oaf.getEntity().getResult();
    if (r.getInstanceCount() > 0) {
      return r.getInstanceList().stream().map(i -> convertInstance(i)).collect(Collectors.toList());
    }
    return Lists.newArrayList();
  }

  private static Instance convertInstance(ResultProtos.Result.Instance ri) {
    final Instance i = new Instance();
    i.setAccessright(mapQualifier(ri.getAccessright()));
    i.setCollectedfrom(mapKV(ri.getCollectedfrom()));
    i.setDateofacceptance(mapStringField(ri.getDateofacceptance()));
    i.setDistributionlocation(ri.getDistributionlocation());
    i.setHostedby(mapKV(ri.getHostedby()));
    i.setInstancetype(mapQualifier(ri.getInstancetype()));
    i.setLicense(mapStringField(ri.getLicense()));
    i.setUrl(ri.getUrlList());
    i.setRefereed(mapStringField(ri.getRefereed()));
    i.setProcessingchargeamount(mapStringField(ri.getProcessingchargeamount()));
    i.setProcessingchargecurrency(mapStringField(ri.getProcessingchargecurrency()));
    return i;
  }

  private static Organization convertOrganization(OafProtos.Oaf oaf) {
    final OrganizationProtos.Organization.Metadata m =
        oaf.getEntity().getOrganization().getMetadata();
    final Organization org = setOaf(new Organization(), oaf);
    setEntity(org, oaf);
    org.setLegalshortname(mapStringField(m.getLegalshortname()));
    org.setLegalname(mapStringField(m.getLegalname()));
    org.setAlternativeNames(
        m.getAlternativeNamesList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    org.setWebsiteurl(mapStringField(m.getWebsiteurl()));
    org.setLogourl(mapStringField(m.getLogourl()));
    org.setEclegalbody(mapStringField(m.getEclegalbody()));
    org.setEclegalperson(mapStringField(m.getEclegalperson()));
    org.setEcnonprofit(mapStringField(m.getEcnonprofit()));
    org.setEcresearchorganization(mapStringField(m.getEcresearchorganization()));
    org.setEchighereducation(mapStringField(m.getEchighereducation()));
    org.setEcinternationalorganizationeurinterests(
        mapStringField(m.getEcinternationalorganizationeurinterests()));
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
    datasource.setAccessinfopackage(
        m.getAccessinfopackageList().stream()
            .map(ProtoConverter::mapStringField)
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
    datasource.setOdcontenttypes(
        m.getOdcontenttypesList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    datasource.setOdlanguages(
        m.getOdlanguagesList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    datasource.setOdnumberofitems(mapStringField(m.getOdnumberofitems()));
    datasource.setOdnumberofitemsdate(mapStringField(m.getOdnumberofitemsdate()));
    datasource.setOdpolicies(mapStringField(m.getOdpolicies()));
    datasource.setOfficialname(mapStringField(m.getOfficialname()));
    datasource.setOpenairecompatibility(mapQualifier(m.getOpenairecompatibility()));
    datasource.setPidsystems(mapStringField(m.getPidsystems()));
    datasource.setPolicies(
        m.getPoliciesList().stream().map(ProtoConverter::mapKV).collect(Collectors.toList()));
    datasource.setQualitymanagementkind(mapStringField(m.getQualitymanagementkind()));
    datasource.setReleaseenddate(mapStringField(m.getReleaseenddate()));
    datasource.setServiceprovider(mapBoolField(m.getServiceprovider()));
    datasource.setReleasestartdate(mapStringField(m.getReleasestartdate()));
    datasource.setSubjects(
        m.getSubjectsList().stream()
            .map(ProtoConverter::mapStructuredProperty)
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
    project.setSubjects(
        m.getSubjectsList().stream()
            .map(sp -> mapStructuredProperty(sp))
            .collect(Collectors.toList()));
    project.setTitle(mapStringField(m.getTitle()));
    project.setWebsiteurl(mapStringField(m.getWebsiteurl()));
    project.setFundingtree(
        m.getFundingtreeList().stream().map(f -> mapStringField(f)).collect(Collectors.toList()));
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
        Result result = setOaf(new Result(), oaf);
        setEntity(result, oaf);
        return setResult(result, oaf);
    }
  }

  private static Software createSoftware(OafProtos.Oaf oaf) {
    ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
    Software software = setOaf(new Software(), oaf);
    setEntity(software, oaf);
    setResult(software, oaf);

    software.setDocumentationUrl(
        m.getDocumentationUrlList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    software.setLicense(
        m.getLicenseList().stream()
            .map(ProtoConverter::mapStructuredProperty)
            .collect(Collectors.toList()));
    software.setCodeRepositoryUrl(mapStringField(m.getCodeRepositoryUrl()));
    software.setProgrammingLanguage(mapQualifier(m.getProgrammingLanguage()));
    return software;
  }

  private static OtherResearchProduct createORP(OafProtos.Oaf oaf) {
    ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
    OtherResearchProduct otherResearchProducts = setOaf(new OtherResearchProduct(), oaf);
    setEntity(otherResearchProducts, oaf);
    setResult(otherResearchProducts, oaf);
    otherResearchProducts.setContactperson(
        m.getContactpersonList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    otherResearchProducts.setContactgroup(
        m.getContactgroupList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    otherResearchProducts.setTool(
        m.getToolList().stream().map(ProtoConverter::mapStringField).collect(Collectors.toList()));

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
    dataset.setStoragedate(mapStringField(m.getStoragedate()));
    dataset.setDevice(mapStringField(m.getDevice()));
    dataset.setSize(mapStringField(m.getSize()));
    dataset.setVersion(mapStringField(m.getVersion()));
    dataset.setLastmetadataupdate(mapStringField(m.getLastmetadataupdate()));
    dataset.setMetadataversionnumber(mapStringField(m.getMetadataversionnumber()));
    dataset.setGeolocation(
        m.getGeolocationList().stream()
            .map(ProtoConverter::mapGeolocation)
            .collect(Collectors.toList()));
    return dataset;
  }

  public static <T extends Oaf> T setOaf(T oaf, OafProtos.Oaf o) {
    oaf.setDataInfo(mapDataInfo(o.getDataInfo()));
    oaf.setLastupdatetimestamp(o.getLastupdatetimestamp());
    return oaf;
  }

  public static <T extends OafEntity> T setEntity(T entity, OafProtos.Oaf oaf) {
    // setting Entity fields
    final OafProtos.OafEntity e = oaf.getEntity();
    entity.setId(e.getId());
    entity.setOriginalId(e.getOriginalIdList());
    entity.setCollectedfrom(
        e.getCollectedfromList().stream().map(ProtoConverter::mapKV).collect(Collectors.toList()));
    entity.setPid(
        e.getPidList().stream()
            .map(ProtoConverter::mapStructuredProperty)
            .collect(Collectors.toList()));
    entity.setDateofcollection(e.getDateofcollection());
    entity.setDateoftransformation(e.getDateoftransformation());
    entity.setExtraInfo(
        e.getExtraInfoList().stream()
            .map(ProtoConverter::mapExtraInfo)
            .collect(Collectors.toList()));
    return entity;
  }

  public static <T extends Result> T setResult(T entity, OafProtos.Oaf oaf) {
    // setting Entity fields
    final ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
    entity.setAuthor(
        m.getAuthorList().stream().map(ProtoConverter::mapAuthor).collect(Collectors.toList()));
    entity.setResulttype(mapQualifier(m.getResulttype()));
    entity.setLanguage(mapQualifier(m.getLanguage()));
    entity.setCountry(
        m.getCountryList().stream()
            .map(ProtoConverter::mapQualifierAsCountry)
            .collect(Collectors.toList()));
    entity.setSubject(
        m.getSubjectList().stream()
            .map(ProtoConverter::mapStructuredProperty)
            .collect(Collectors.toList()));
    entity.setTitle(
        m.getTitleList().stream()
            .map(ProtoConverter::mapStructuredProperty)
            .collect(Collectors.toList()));
    entity.setRelevantdate(
        m.getRelevantdateList().stream()
            .map(ProtoConverter::mapStructuredProperty)
            .collect(Collectors.toList()));
    entity.setDescription(
        m.getDescriptionList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setDateofacceptance(mapStringField(m.getDateofacceptance()));
    entity.setPublisher(mapStringField(m.getPublisher()));
    entity.setEmbargoenddate(mapStringField(m.getEmbargoenddate()));
    entity.setSource(
        m.getSourceList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setFulltext(
        m.getFulltextList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setFormat(
        m.getFormatList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setContributor(
        m.getContributorList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setResourcetype(mapQualifier(m.getResourcetype()));
    entity.setCoverage(
        m.getCoverageList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    entity.setContext(
        m.getContextList().stream().map(ProtoConverter::mapContext).collect(Collectors.toList()));

    entity.setBestaccessright(getBestAccessRights(oaf.getEntity().getResult().getInstanceList()));

    return entity;
  }

  private static Qualifier getBestAccessRights(List<ResultProtos.Result.Instance> instanceList) {
    if (instanceList != null) {
      final Optional<FieldTypeProtos.Qualifier> min =
          instanceList.stream().map(i -> i.getAccessright()).min(new LicenseComparator());

      final Qualifier rights = min.isPresent() ? mapQualifier(min.get()) : new Qualifier();

      if (StringUtils.isBlank(rights.getClassid())) {
        rights.setClassid(UNKNOWN);
      }
      if (StringUtils.isBlank(rights.getClassname())
          || UNKNOWN.equalsIgnoreCase(rights.getClassname())) {
        rights.setClassname(NOT_AVAILABLE);
      }
      if (StringUtils.isBlank(rights.getSchemeid())) {
        rights.setSchemeid(DNET_ACCESS_MODES);
      }
      if (StringUtils.isBlank(rights.getSchemename())) {
        rights.setSchemename(DNET_ACCESS_MODES);
      }

      return rights;
    }
    return null;
  }

  private static Context mapContext(ResultProtos.Result.Context context) {

    final Context entity = new Context();
    entity.setId(context.getId());
    entity.setDataInfo(
        context.getDataInfoList().stream()
            .map(ProtoConverter::mapDataInfo)
            .collect(Collectors.toList()));
    return entity;
  }

  public static KeyValue mapKV(FieldTypeProtos.KeyValue kv) {
    final KeyValue keyValue = new KeyValue();
    keyValue.setKey(kv.getKey());
    keyValue.setValue(kv.getValue());
    keyValue.setDataInfo(mapDataInfo(kv.getDataInfo()));
    return keyValue;
  }

  public static DataInfo mapDataInfo(FieldTypeProtos.DataInfo d) {
    final DataInfo dataInfo = new DataInfo();
    dataInfo.setDeletedbyinference(d.getDeletedbyinference());
    dataInfo.setInferenceprovenance(d.getInferenceprovenance());
    dataInfo.setInferred(d.getInferred());
    dataInfo.setInvisible(d.getInvisible());
    dataInfo.setProvenanceaction(mapQualifier(d.getProvenanceaction()));
    dataInfo.setTrust(d.getTrust());
    return dataInfo;
  }

  public static Qualifier mapQualifier(FieldTypeProtos.Qualifier q) {
    final Qualifier qualifier = new Qualifier();
    qualifier.setClassid(q.getClassid());
    qualifier.setClassname(q.getClassname());
    qualifier.setSchemeid(q.getSchemeid());
    qualifier.setSchemename(q.getSchemename());
    return qualifier;
  }

  public static Country mapQualifierAsCountry(FieldTypeProtos.Qualifier q) {
    final Country c = new Country();
    c.setClassid(q.getClassid());
    c.setClassname(q.getClassname());
    c.setSchemeid(q.getSchemeid());
    c.setSchemename(q.getSchemename());
    c.setDataInfo(mapDataInfo(q.getDataInfo()));
    return c;
  }

  public static StructuredProperty mapStructuredProperty(FieldTypeProtos.StructuredProperty sp) {
    final StructuredProperty structuredProperty = new StructuredProperty();
    structuredProperty.setValue(sp.getValue());
    structuredProperty.setQualifier(mapQualifier(sp.getQualifier()));
    structuredProperty.setDataInfo(mapDataInfo(sp.getDataInfo()));
    return structuredProperty;
  }

  public static ExtraInfo mapExtraInfo(FieldTypeProtos.ExtraInfo extraInfo) {
    final ExtraInfo entity = new ExtraInfo();
    entity.setName(extraInfo.getName());
    entity.setTypology(extraInfo.getTypology());
    entity.setProvenance(extraInfo.getProvenance());
    entity.setTrust(extraInfo.getTrust());
    entity.setValue(extraInfo.getValue());
    return entity;
  }

  public static OAIProvenance mapOAIProvenance(FieldTypeProtos.OAIProvenance oaiProvenance) {
    final OAIProvenance entity = new OAIProvenance();
    entity.setOriginDescription(mapOriginalDescription(oaiProvenance.getOriginDescription()));
    return entity;
  }

  public static OriginDescription mapOriginalDescription(
      FieldTypeProtos.OAIProvenance.OriginDescription originDescription) {
    final OriginDescription originDescriptionResult = new OriginDescription();
    originDescriptionResult.setHarvestDate(originDescription.getHarvestDate());
    originDescriptionResult.setAltered(originDescription.getAltered());
    originDescriptionResult.setBaseURL(originDescription.getBaseURL());
    originDescriptionResult.setIdentifier(originDescription.getIdentifier());
    originDescriptionResult.setDatestamp(originDescription.getDatestamp());
    originDescriptionResult.setMetadataNamespace(originDescription.getMetadataNamespace());
    return originDescriptionResult;
  }

  public static Field<String> mapStringField(FieldTypeProtos.StringField s) {
    final Field<String> stringField = new Field<>();
    stringField.setValue(s.getValue());
    stringField.setDataInfo(mapDataInfo(s.getDataInfo()));
    return stringField;
  }

  public static Field<Boolean> mapBoolField(FieldTypeProtos.BoolField b) {
    final Field<Boolean> booleanField = new Field<>();
    booleanField.setValue(b.getValue());
    booleanField.setDataInfo(mapDataInfo(b.getDataInfo()));
    return booleanField;
  }

  public static Field<Integer> mapIntField(FieldTypeProtos.IntField b) {
    final Field<Integer> entity = new Field<>();
    entity.setValue(b.getValue());
    entity.setDataInfo(mapDataInfo(b.getDataInfo()));
    return entity;
  }

  public static Journal mapJournal(FieldTypeProtos.Journal j) {
    final Journal journal = new Journal();
    journal.setConferencedate(j.getConferencedate());
    journal.setConferenceplace(j.getConferenceplace());
    journal.setEdition(j.getEdition());
    journal.setEp(j.getEp());
    journal.setIss(j.getIss());
    journal.setIssnLinking(j.getIssnLinking());
    journal.setIssnOnline(j.getIssnOnline());
    journal.setIssnPrinted(j.getIssnPrinted());
    journal.setName(j.getName());
    journal.setSp(j.getSp());
    journal.setVol(j.getVol());
    journal.setDataInfo(mapDataInfo(j.getDataInfo()));
    return journal;
  }

  public static Author mapAuthor(FieldTypeProtos.Author author) {
    final Author entity = new Author();
    entity.setFullname(author.getFullname());
    entity.setName(author.getName());
    entity.setSurname(author.getSurname());
    entity.setRank(author.getRank());
    entity.setPid(
        author.getPidList().stream()
            .map(
                kv -> {
                  final StructuredProperty sp = new StructuredProperty();
                  sp.setValue(kv.getValue());
                  final Qualifier q = new Qualifier();
                  q.setClassid(kv.getKey());
                  q.setClassname(kv.getKey());
                  sp.setQualifier(q);
                  return sp;
                })
            .collect(Collectors.toList()));
    entity.setAffiliation(
        author.getAffiliationList().stream()
            .map(ProtoConverter::mapStringField)
            .collect(Collectors.toList()));
    return entity;
  }

  public static GeoLocation mapGeolocation(ResultProtos.Result.GeoLocation geoLocation) {
    final GeoLocation entity = new GeoLocation();
    entity.setPoint(geoLocation.getPoint());
    entity.setBox(geoLocation.getBox());
    entity.setPlace(geoLocation.getPlace());
    return entity;
  }
}
