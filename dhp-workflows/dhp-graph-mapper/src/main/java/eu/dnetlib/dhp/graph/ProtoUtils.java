package eu.dnetlib.dhp.graph;

import com.googlecode.protobuf.format.JsonFormat;
import eu.dnetlib.data.proto.FieldTypeProtos;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.dhp.schema.oaf.*;

public class ProtoUtils {

    public static OafProtos.Oaf parse(String json) throws JsonFormat.ParseException {
        final OafProtos.Oaf.Builder builder =  OafProtos.Oaf.newBuilder();
        JsonFormat.merge(json, builder);
        return builder.build();
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
                .setSchemename(q.getSchemename())
                .setDataInfo(q.hasDataInfo() ? mapDataInfo(q.getDataInfo()) : null);
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

}
