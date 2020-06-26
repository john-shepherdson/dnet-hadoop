
package eu.dnetlib.doiboost.orcidnodoi.proto;

public class ProtoWriter {

}
//
//import static eu.dnetlib.data.mapreduce.hbase.dataimport.DumpToActionsUtility.getArrayValues;
//import static eu.dnetlib.data.mapreduce.hbase.dataimport.DumpToActionsUtility.getDefaultResulttype;
//import static eu.dnetlib.data.mapreduce.hbase.dataimport.DumpToActionsUtility.getQualifier;
//import static eu.dnetlib.data.mapreduce.hbase.dataimport.DumpToActionsUtility.getStringValue;
//import static eu.dnetlib.data.mapreduce.hbase.dataimport.DumpToActionsUtility.isValidDate;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.lang3.StringUtils;
//
//import com.google.gson.Gson;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.googlecode.protobuf.format.JsonFormat;
//
//import eu.dnetlib.actionmanager.actions.ActionFactory;
//import eu.dnetlib.actionmanager.actions.AtomicAction;
//import eu.dnetlib.actionmanager.common.Agent;
//import eu.dnetlib.data.mapreduce.hbase.Reporter;
//import eu.dnetlib.data.mapreduce.util.StreamUtils;
//import eu.dnetlib.data.proto.FieldTypeProtos;
//import eu.dnetlib.data.proto.FieldTypeProtos.Author;
//import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
//import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
//import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
//import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
//import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
//import eu.dnetlib.data.proto.KindProtos;
//import eu.dnetlib.data.proto.OafProtos;
//import eu.dnetlib.data.proto.ResultProtos;
//import eu.dnetlib.data.proto.TypeProtos;
//import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
//import eu.dnetlib.miscutils.collections.Pair;
//import eu.dnetlib.miscutils.datetime.DateUtils;
//import eu.dnetlib.pace.model.Person;
//
//public class ProtoWriter {
//
//    public static final String ORCID = "ORCID";
//    public final static String orcidPREFIX = "orcid_______";
//    public static final String OPENAIRE_PREFIX = "openaire____";
//    public static final String SEPARATOR = "::";
//
//    private static Map<String, Pair<String, String>> datasources = new HashMap<String, Pair<String, String>>() {
//
//        {
//            put(ORCID.toLowerCase(), new Pair<>(ORCID, OPENAIRE_PREFIX + SEPARATOR + "orcid"));
//
//        }
//    };
//
//    // json external id will be mapped to oaf:pid/@classid Map to oaf:pid/@classname
//    private static Map<String, Pair<String, String>> externalIds = new HashMap<String, Pair<String, String>>() {
//
//        {
//            put("ark".toLowerCase(), new Pair<>("ark", "ark"));
//            put("arxiv".toLowerCase(), new Pair<>("arxiv", "arXiv"));
//            put("pmc".toLowerCase(), new Pair<>("pmc", "pmc"));
//            put("pmid".toLowerCase(), new Pair<>("pmid", "pmid"));
//            put("source-work-id".toLowerCase(), new Pair<>("orcidworkid", "orcidworkid"));
//            put("urn".toLowerCase(), new Pair<>("urn", "urn"));
//        }
//    };
//
//    static Map<String, Map<String, String>> typologiesMapping;
//
//    static {
//        try {
//            final InputStream is = OrcidToActions.class.getResourceAsStream("/eu/dnetlib/data/mapreduce/hbase/dataimport/mapping_typologies_orcid.json");
//            final String tt = IOUtils.toString(is);
//            typologiesMapping = new Gson().fromJson(tt, Map.class);
//        } catch (final IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static final String PID_TYPES = "dnet:pid_types";
//
//    public static List<AtomicAction> generatePublicationActionsFromDump(final JsonObject rootElement,
//                                                                        final ActionFactory factory,
//                                                                        final String setName,
//                                                                        final Agent agent,
//                                                                        final Reporter context) {
//
//        if (!isValid(rootElement, context)) { return null; }
//
//        // Create OAF proto
//
//        final OafProtos.Oaf.Builder oaf = OafProtos.Oaf.newBuilder();
//
//        oaf.setDataInfo(
//                DataInfo.newBuilder()
//                        .setDeletedbyinference(false)
//                        .setInferred(false)
//                        .setTrust("0.9")
//                        .setProvenanceaction(getQualifier("sysimport:actionset:orcidworks-no-doi", "dnet:provenanceActions"))
//                        .build());
//
//        // Adding kind
//        oaf.setKind(KindProtos.Kind.entity);
//
//        oaf.setLastupdatetimestamp(DateUtils.now());
//
//        // creating result proto
//        final OafProtos.OafEntity.Builder entity = OafProtos.OafEntity.newBuilder().setType(TypeProtos.Type.result);
//
//        entity.setDateofcollection("2018-10-22");
//        entity.setDateoftransformation(DateUtils.now_ISO8601());
//
//        // Adding external ids
//        StreamUtils.toStream(externalIds.keySet().iterator())
//                .forEach(jsonExtId -> {
//                    final String classid = externalIds.get(jsonExtId.toLowerCase()).getValue();
//                    final String classname = externalIds.get(jsonExtId.toLowerCase()).getKey();
//                    final String extId = getStringValue(rootElement, jsonExtId);
//                    if (StringUtils.isNotBlank(extId)) {
//                        entity.addPid(StructuredProperty.newBuilder()
//                                .setValue(extId)
//                                .setQualifier(Qualifier.newBuilder().setClassid(classid).setClassname(classname).setSchemeid("dnet:pid_types")
//                                        .setSchemename("dnet:pid_types").build())
//                                .build());
//                    }
//                });
//
//        // Create result field
//        final ResultProtos.Result.Builder result = ResultProtos.Result.newBuilder();
//
//        // Create metadata proto
//        final ResultProtos.Result.Metadata.Builder metadata = ResultProtos.Result.Metadata.newBuilder();
//
//        // Adding source
//        final String source = getStringValue(rootElement, "source");
//        if (StringUtils.isNotBlank(source)) {
//            metadata.addSource(StringField.newBuilder().setValue(source).build());
//        }
//
//        // Adding title
//        final String title = createRepeatedField(rootElement, "titles");
//        if (StringUtils.isBlank(title)) {
//            context.incrementCounter("filtered", "title_not_found", 1);
//            return null;
//        }
//        metadata.addTitle(FieldTypeProtos.StructuredProperty.newBuilder()
//                .setValue(title)
//                .setQualifier(getQualifier("main title", "dnet:dataCite_title"))
//                .build());
//
//        // Adding identifier
//        final String id = getStringValue(rootElement, "id");
//        String sourceId = null;
//        if (id != null) {
//            entity.addOriginalId(id);
//            sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, AbstractDNetXsltFunctions.md5(id));
//        } else {
//            sourceId = String.format("50|%s" + SEPARATOR + "%s", orcidPREFIX, AbstractDNetXsltFunctions.md5(title));
//        }
//        entity.setId(sourceId);
//
//        // Adding relevant date
//        settingRelevantDate(rootElement, metadata, "publication_date", "issued", true);
//
//        // Adding collectedfrom
//        final FieldTypeProtos.KeyValue collectedFrom = FieldTypeProtos.KeyValue.newBuilder()
//                .setValue(ORCID)
//                .setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + "806360c771262b4d6770e7cdf04b5c5a")
//                .build();
//        entity.addCollectedfrom(collectedFrom);
//
//        // Adding type
//        final String type = getStringValue(rootElement, "type");
//        String cobjValue = "";
//        if (StringUtils.isNotBlank(type)) {
//
//            metadata.setResourcetype(FieldTypeProtos.Qualifier.newBuilder()
//                    .setClassid(type)
//                    .setClassname(type)
//                    .setSchemeid("dnet:dataCite_resource")
//                    .setSchemename("dnet:dataCite_resource")
//                    .build());
//
//            final String typeValue = typologiesMapping.get(type).get("value");
//            cobjValue = typologiesMapping.get(type).get("cobj");
//            final ResultProtos.Result.Instance.Builder instance = ResultProtos.Result.Instance.newBuilder();
//
//            // Adding hostedby
//            instance.setHostedby(FieldTypeProtos.KeyValue.newBuilder()
//                    .setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + "55045bd2a65019fd8e6741a755395c8c")
//                    .setValue("Unknown Repository")
//                    .build());
//
//            // Adding url
//            final String url = createRepeatedField(rootElement, "urls");
//            if (StringUtils.isNotBlank(url)) {
//                instance.addUrl(url);
//            }
//
//            final String pubDate = getPublicationDate(rootElement, "publication_date");
//            if (StringUtils.isNotBlank(pubDate)) {
//                instance.setDateofacceptance(FieldTypeProtos.StringField.newBuilder().setValue(pubDate).build());
//            }
//
//            instance.setCollectedfrom(collectedFrom);
//
//            // Adding accessright
//            instance.setAccessright(FieldTypeProtos.Qualifier.newBuilder()
//                    .setClassid("UNKNOWN")
//                    .setClassname("UNKNOWN")
//                    .setSchemeid("dnet:access_modes")
//                    .setSchemename("dnet:access_modes")
//                    .build());
//
//            // Adding type
//            instance.setInstancetype(FieldTypeProtos.Qualifier.newBuilder()
//                    .setClassid(cobjValue)
//                    .setClassname(typeValue)
//                    .setSchemeid("dnet:publication_resource")
//                    .setSchemename("dnet:publication_resource")
//                    .build());
//
//            result.addInstance(instance);
//        } else {
//            context.incrementCounter("filtered", "type_not_found", 1);
//            return null;
//        }
//
//        // Adding authors
//        final List<Author> authors = createAuthors(rootElement);
//        if (authors != null && authors.size() > 0) {
//            metadata.addAllAuthor(authors);
//        } else {
//            context.incrementCounter("filtered", "author_not_found", 1);
//            return null;
//        }
//
//        metadata.setResulttype(getQualifier(getDefaultResulttype(cobjValue), "dnet:result_typologies"));
//        result.setMetadata(metadata.build());
//        entity.setResult(result.build());
//        oaf.setEntity(entity.build());
//
//        final List<AtomicAction> actionList = new ArrayList<>();
//
//        actionList.add(factory.createAtomicAction(setName, agent, oaf.getEntity().getId(), "result", "body", oaf.build().toByteArray()));
//
////		 System.out.println(JsonFormat.printToString(oaf.build()));
//        return actionList;
//
//    }
//
//    public static List<Author> createAuthors(final JsonObject root) {
//
//        final String authorsJSONFieldName = "authors";
//
//        if (root.has(authorsJSONFieldName) && root.get(authorsJSONFieldName).isJsonArray()) {
//
//            final List<Author> authors = new ArrayList<>();
//            final JsonArray jsonAuthors = root.getAsJsonArray(authorsJSONFieldName);
//            int firstCounter = 0;
//            int defaultCounter = 0;
//            int rank = 1;
//            int currentRank = 0;
//
//            for (final JsonElement item : jsonAuthors) {
//                final JsonObject author = item.getAsJsonObject();
//                final Author.Builder result = Author.newBuilder();
//                if (item.isJsonObject()) {
//                    final String surname = getStringValue(author, "surname");
//                    final String name = getStringValue(author, "name");
//                    final String oid = getStringValue(author, "oid");
//                    final String seq = getStringValue(author, "seq");
//                    if (StringUtils.isNotBlank(seq)) {
//                        if (seq.equals("first")) {
//                            firstCounter += 1;
//                            rank = firstCounter;
//
//                        } else if (seq.equals("additional")) {
//                            rank = currentRank + 1;
//                        } else {
//                            defaultCounter += 1;
//                            rank = defaultCounter;
//                        }
//                    }
//
//                    if (StringUtils.isNotBlank(oid)) {
//                        result.addPid(KeyValue.newBuilder()
//                                .setValue(oid)
//                                .setKey("ORCID")
//                                .build());
//                        result.setFullname(name + " " + surname);
//                        if (StringUtils.isNotBlank(name)) {
//                            result.setName(name);
//                        }
//                        if (StringUtils.isNotBlank(surname)) {
//                            result.setSurname(surname);
//                        }
//                    } else {
//                        String fullname = "";
//                        if (StringUtils.isNotBlank(name)) {
//                            fullname = name;
//                        } else {
//                            if (StringUtils.isNotBlank(surname)) {
//                                fullname = surname;
//                            }
//                        }
//                        Person p = new Person(fullname, false);
//                        if (p.isAccurate()) {
//                            result.setName(p.getNormalisedFirstName());
//                            result.setSurname(p.getNormalisedSurname());
//                            result.setFullname(p.getNormalisedFullname());
//                        }
//                        else {
//                            result.setFullname(fullname);
//                        }
//                    }
//                }
//                result.setRank(rank);
//                authors.add(result.build());
//                currentRank = rank;
//            }
//            return authors;
//
//        }
//        return null;
//    }
//
//    private static String createRepeatedField(final JsonObject rootElement, final String fieldName) {
//        String field = "";
//        if (!rootElement.has(fieldName)) { return null; }
//        if (rootElement.has(fieldName) && rootElement.get(fieldName).isJsonNull()) { return null; }
//        if (rootElement.get(fieldName).isJsonArray()) {
//            if (!isValidJsonArray(rootElement, fieldName)) { return null; }
//            final StringBuilder ttl = new StringBuilder();
//            getArrayValues(rootElement, fieldName).forEach(ttl::append);
//            field = ttl.toString();
//        } else {
//            field = getStringValue(rootElement, fieldName);
//        }
//
//        if (field != null && !field.isEmpty() && field.charAt(0) == '"' && field.charAt(field.length() - 1) == '"') {
//            field = field.substring(1, field.length() - 1);
//        }
//        return field;
//    }
//
//    private static void settingRelevantDate(final JsonObject rootElement,
//                                            final ResultProtos.Result.Metadata.Builder metadata,
//                                            final String jsonKey,
//                                            final String dictionaryKey,
//                                            final boolean addToDateOfAcceptance) {
//
//        final String pubDate = getPublicationDate(rootElement, "publication_date");
//        if (StringUtils.isNotBlank(pubDate)) {
//            if (addToDateOfAcceptance) {
//                metadata.setDateofacceptance(FieldTypeProtos.StringField.newBuilder().setValue(pubDate).build());
//            }
//            metadata.addRelevantdate(FieldTypeProtos.StructuredProperty.newBuilder()
//                    .setValue(pubDate)
//                    .setQualifier(getQualifier(dictionaryKey, "dnet:dataCite_date"))
//                    .build());
//        }
//    }
//
//    private static String getPublicationDate(final JsonObject rootElement,
//                                             final String jsonKey) {
//
//        final JsonObject pubDateJson = rootElement.getAsJsonObject(jsonKey);
//        if (pubDateJson == null) { return null; }
//        final String year = getStringValue(pubDateJson, "year");
//        final String month = getStringValue(pubDateJson, "month");
//        final String day = getStringValue(pubDateJson, "day");
//
//        if (StringUtils.isBlank(year)) { return null; }
//        String pubDate = "".concat(year);
//        if (StringUtils.isNotBlank(month)) {
//            pubDate = pubDate.concat("-" + month);
//            if (StringUtils.isNotBlank(day)) {
//                pubDate = pubDate.concat("-" + day);
//            } else {
//                pubDate += "-01";
//            }
//        } else {
//            pubDate += "-01-01";
//        }
//        if (isValidDate(pubDate)) { return pubDate; }
//        return null;
//    }
//
//    protected static boolean isValid(final JsonObject rootElement, final Reporter context) {
//
//        final String type = getStringValue(rootElement, "type");
//        if (!typologiesMapping.containsKey(type)) {
//            context.incrementCounter("filtered", "unknowntype_" + type, 1);
//            return false;
//        }
//
//        if (!isValidJsonArray(rootElement, "titles")) {
//            context.incrementCounter("filtered", "invalid_title", 1);
//            return false;
//        }
//        return true;
//    }
//
//    private static boolean isValidJsonArray(final JsonObject rootElement, final String fieldName) {
//        if (!rootElement.has(fieldName)) { return false; }
//        final JsonElement jsonElement = rootElement.get(fieldName);
//        if (jsonElement.isJsonNull()) { return false; }
//        if (jsonElement.isJsonArray()) {
//            final JsonArray jsonArray = jsonElement.getAsJsonArray();
//            if (jsonArray.isJsonNull()) { return false; }
//            if (jsonArray.get(0).isJsonNull()) { return false; }
//        }
//        return true;
//    }
//}
