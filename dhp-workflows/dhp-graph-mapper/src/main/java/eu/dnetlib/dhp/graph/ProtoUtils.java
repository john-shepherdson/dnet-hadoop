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
        oaf.setDataInfo(mapDataInfo(o.getDataInfo())).setLastupdatetimestamp(o.getLastupdatetimestamp());
        return oaf;
    }

    public static <T extends OafEntity> T setEntity(T entity, OafProtos.Oaf oaf) {
        //setting Entity fields
        final OafProtos.OafEntity e = oaf.getEntity();
        entity
                .setId(e.getId())
                .setOriginalId(e.getOriginalIdList())
                .setCollectedfrom(e.getCollectedfromList()
                        .stream()
                        .map(ProtoUtils::mapKV)
                        .collect(Collectors.toList()))
                .setPid(e.getPidList().stream()
                        .map(ProtoUtils::mapStructuredProperty)
                        .collect(Collectors.toList()))
                .setDateofcollection(entity.getDateofcollection())
                .setDateoftransformation(entity.getDateoftransformation())
                .setExtraInfo(e.getExtraInfoList()
                        .stream()
                        .map(ProtoUtils::mapExtraInfo)
                        .collect(Collectors.toList()));
        return entity;
    }

    public static <T extends Result> T setResult(T entity, OafProtos.Oaf oaf) {
        //setting Entity fields
        final ResultProtos.Result.Metadata m = oaf.getEntity().getResult().getMetadata();
        entity
                .setAuthor(m.getAuthorList()
                        .stream()
                        .map(ProtoUtils::mapAuthor)
                        .collect(Collectors.toList()))
                .setResulttype(mapQualifier(m.getResulttype()))
                .setLanguage(ProtoUtils.mapQualifier(m.getLanguage()))
                .setCountry(m.getCountryList()
                        .stream()
                        .map(ProtoUtils::mapQualifier)
                        .collect(Collectors.toList()))
                .setSubject(m.getSubjectList()
                        .stream()
                        .map(ProtoUtils::mapStructuredProperty)
                        .collect(Collectors.toList()))
                .setTitle(m.getTitleList()
                        .stream()
                        .map(ProtoUtils::mapStructuredProperty)
                        .collect(Collectors.toList()))
                .setRelevantdate(m.getRelevantdateList()
                        .stream()
                        .map(ProtoUtils::mapStructuredProperty)
                        .collect(Collectors.toList()))
                .setDescription(m.getDescriptionList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setDateofacceptance(ProtoUtils.mapStringField(m.getDateofacceptance()))
                .setPublisher(ProtoUtils.mapStringField(m.getPublisher()))
                .setEmbargoenddate(ProtoUtils.mapStringField(m.getEmbargoenddate()))
                .setSource(m.getSourceList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setFulltext(m.getFulltextList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setFormat(m.getFormatList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setContributor(m.getContributorList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setResourcetype(ProtoUtils.mapQualifier(m.getResourcetype()))
                .setCoverage(m.getCoverageList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()))
                .setRefereed(mapStringField(m.getRefereed()))
                .setContext(m.getContextList()
                        .stream()
                        .map(ProtoUtils::mapContext)
                        .collect(Collectors.toList()));

        return entity;
    }

    private static Context mapContext(ResultProtos.Result.Context context) {

        return new Context()
                .setId(context.getId())
                .setDataInfo(context.getDataInfoList()
                        .stream()
                        .map(ProtoUtils::mapDataInfo)
                        .collect(Collectors.toList()));
    }


    public static KeyValue mapKV(FieldTypeProtos.KeyValue kv) {
        return new KeyValue()
                .setKey(kv.getKey())
                .setValue(kv.getValue())
                .setDataInfo(mapDataInfo(kv.getDataInfo()));
    }

    public static DataInfo mapDataInfo(FieldTypeProtos.DataInfo d) {
        return new DataInfo()
                .setDeletedbyinference(d.getDeletedbyinference())
                .setInferenceprovenance(d.getInferenceprovenance())
                .setInferred(d.getInferred())
                .setInvisible(d.getInvisible())
                .setProvenanceaction(mapQualifier(d.getProvenanceaction()));
    }

    public static Qualifier mapQualifier(FieldTypeProtos.Qualifier q) {
        return new Qualifier()
                .setClassid(q.getClassid())
                .setClassname(q.getClassname())
                .setSchemeid(q.getSchemeid())
                .setSchemename(q.getSchemename());
        //.setDataInfo(q.hasDataInfo() ? mapDataInfo(q.getDataInfo()) : null);
    }

    public static StructuredProperty mapStructuredProperty(FieldTypeProtos.StructuredProperty sp) {
        return new StructuredProperty()
                .setValue(sp.getValue())
                .setQualifier(mapQualifier(sp.getQualifier()))
                .setDataInfo(sp.hasDataInfo() ? mapDataInfo(sp.getDataInfo()) : null);
    }

    public static ExtraInfo mapExtraInfo(FieldTypeProtos.ExtraInfo extraInfo) {
        return new ExtraInfo()
                .setName(extraInfo.getName())
                .setTypology(extraInfo.getTypology())
                .setProvenance(extraInfo.getProvenance())
                .setTrust(extraInfo.getTrust())
                .setValue(extraInfo.getValue());
    }

    public static OAIProvenance mapOAIProvenance(FieldTypeProtos.OAIProvenance oaiProvenance) {
        return new OAIProvenance().setOriginDescription(mapOriginalDescription(oaiProvenance.getOriginDescription()));
    }

    public static OriginDescription mapOriginalDescription(FieldTypeProtos.OAIProvenance.OriginDescription originDescription) {
        final OriginDescription originDescriptionResult = new OriginDescription()
                .setHarvestDate(originDescription.getHarvestDate())
                .setAltered(originDescription.getAltered())
                .setBaseURL(originDescription.getBaseURL())
                .setIdentifier(originDescription.getIdentifier())
                .setDatestamp(originDescription.getDatestamp())
                .setMetadataNamespace(originDescription.getMetadataNamespace());
//        if (originDescription.hasOriginDescription())
//            originDescriptionResult.setOriginDescription(mapOriginalDescription(originDescription.getOriginDescription()));
        return originDescriptionResult;
    }

    public static Field<String> mapStringField(FieldTypeProtos.StringField s) {
        return new Field<String>()
                .setValue(s.getValue())
                .setDataInfo(s.hasDataInfo() ? mapDataInfo(s.getDataInfo()) : null);
    }

    public static Field<Boolean> mapBoolField(FieldTypeProtos.BoolField b) {
        return new Field<Boolean>()
                .setValue(b.getValue())
                .setDataInfo(b.hasDataInfo() ? mapDataInfo(b.getDataInfo()) : null);
    }

    public static Field<Integer> mapIntField(FieldTypeProtos.IntField b) {
        return new Field<Integer>()
                .setValue(b.getValue())
                .setDataInfo(b.hasDataInfo() ? mapDataInfo(b.getDataInfo()) : null);
    }

    public static Journal mapJournal(FieldTypeProtos.Journal j) {
        return new Journal()
                .setConferencedate(j.getConferencedate())
                .setConferenceplace(j.getConferenceplace())
                .setEdition(j.getEdition())
                .setEp(j.getEp())
                .setIss(j.getIss())
                .setIssnLinking(j.getIssnLinking())
                .setIssnOnline(j.getIssnOnline())
                .setIssnPrinted(j.getIssnPrinted())
                .setName(j.getName())
                .setSp(j.getSp())
                .setVol(j.getVol())
                .setDataInfo(mapDataInfo(j.getDataInfo()));
    }

    public static Author mapAuthor(FieldTypeProtos.Author author) {
        return new Author()
                .setFullname(author.getFullname())
                .setName(author.getName())
                .setSurname(author.getSurname())
                .setRank(author.getRank())
                .setPid(author.getPidList()
                        .stream()
                        .map(ProtoUtils::mapKV)
                        .collect(Collectors.toList()))
                .setAffiliation(author.getAffiliationList()
                        .stream()
                        .map(ProtoUtils::mapStringField)
                        .collect(Collectors.toList()));

    }

}
