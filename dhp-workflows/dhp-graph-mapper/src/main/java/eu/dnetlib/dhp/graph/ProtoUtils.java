package eu.dnetlib.dhp.graph;

import com.googlecode.protobuf.format.JsonFormat;
import eu.dnetlib.data.proto.FieldTypeProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.ResultProtos;
import eu.dnetlib.dhp.schema.oaf.*;

import java.util.stream.Collectors;

public class ProtoUtils {

    public static OafProtos.Oaf parse(String json) throws JsonFormat.ParseException {
        final OafProtos.Oaf.Builder builder = OafProtos.Oaf.newBuilder();
        JsonFormat.merge(json, builder);
        return builder.build();
    }

    public static <T extends Oaf> T setOaf(T oaf, OafProtos.Oaf o) {
        oaf.setDataInfo(mapDataInfo(o.getDataInfo()));
        oaf.setLastupdatetimestamp(o.getLastupdatetimestamp());
        return oaf;
    }

    public static <T extends OafEntity> T setEntity(T entity, OafProtos.Oaf oaf) {
        //setting Entity fields
        final OafProtos.OafEntity e = oaf.getEntity();
        entity.setId(e.getId());
        entity.setOriginalId(e.getOriginalIdList());
        entity.setCollectedfrom(e.getCollectedfromList()
                .stream()
                .map(ProtoUtils::mapKV)
                .collect(Collectors.toList()));
        entity.setPid(e.getPidList().stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        entity.setDateofcollection(entity.getDateofcollection());
        entity.setDateoftransformation(entity.getDateoftransformation());
        entity.setExtraInfo(e.getExtraInfoList()
                .stream()
                .map(ProtoUtils::mapExtraInfo)
                .collect(Collectors.toList()));
        return entity;
    }

    public static <T extends Result> T setResult(T entity, OafProtos.Oaf oaf) {
        //setting Entity fields
        final ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        entity.setAuthor(m.getAuthorList()
                .stream()
                .map(ProtoUtils::mapAuthor)
                .collect(Collectors.toList()));
        entity.setResulttype(mapQualifier(m.getResulttype()));
        entity.setLanguage(ProtoUtils.mapQualifier(m.getLanguage()));
        entity.setCountry(m.getCountryList()
                .stream()
                .map(ProtoUtils::mapQualifier)
                .collect(Collectors.toList()));
        entity.setSubject(m.getSubjectList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        entity.setTitle(m.getTitleList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        entity.setRelevantdate(m.getRelevantdateList()
                .stream()
                .map(ProtoUtils::mapStructuredProperty)
                .collect(Collectors.toList()));
        entity.setDescription(m.getDescriptionList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setDateofacceptance(ProtoUtils.mapStringField(m.getDateofacceptance()));
        entity.setPublisher(ProtoUtils.mapStringField(m.getPublisher()));
        entity.setEmbargoenddate(ProtoUtils.mapStringField(m.getEmbargoenddate()));
        entity.setSource(m.getSourceList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setFulltext(m.getFulltextList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setFormat(m.getFormatList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setContributor(m.getContributorList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setResourcetype(ProtoUtils.mapQualifier(m.getResourcetype()));
        entity.setCoverage(m.getCoverageList()
                .stream()
                .map(ProtoUtils::mapStringField)
                .collect(Collectors.toList()));
        entity.setRefereed(mapStringField(m.getRefereed()));
        entity.setContext(m.getContextList()
                .stream()
                .map(ProtoUtils::mapContext)
                .collect(Collectors.toList()));

        return entity;
    }

    private static Context mapContext(ResultProtos.Result.Context context) {

        final Context entity = new Context();
        entity.setId(context.getId());
        entity.setDataInfo(context.getDataInfoList()
                .stream()
                .map(ProtoUtils::mapDataInfo)
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

    public static OriginDescription mapOriginalDescription(FieldTypeProtos.OAIProvenance.OriginDescription originDescription) {
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
        entity.setPid(author.getPidList()
                .stream()
                .map(ProtoUtils::mapKV)
                .collect(Collectors.toList()));
        entity.setAffiliation(author.getAffiliationList()
                .stream()
                .map(ProtoUtils::mapStringField)
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
