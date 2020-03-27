package eu.dnetlib.dhp.sx.graph.parser;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.parser.utility.VtdUtilityParser.Node;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication;
import eu.dnetlib.dhp.schema.scholexplorer.ProvenaceInfo;
import eu.dnetlib.scholexplorer.relation.RelInfo;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PublicationScholexplorerParser extends AbstractScholexplorerParser {

    @Override
    public List<Oaf> parseObject(final String record, final RelationMapper relationMapper) {
        try {
            final List<Oaf> result = new ArrayList<>();
            final DLIPublication parsedObject = new DLIPublication();
            final VTDGen vg = new VTDGen();
            vg.setDoc(record.getBytes());
            vg.parse(true);


            final VTDNav vn = vg.getNav();
            final AutoPilot ap = new AutoPilot(vn);

            final DataInfo di = new DataInfo();
            di.setTrust("0.9");
            di.setDeletedbyinference(false);
            di.setInvisible(false);

            parsedObject.setDateofcollection(VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='dateOfCollection']"));

            final String resolvedDate = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='resolvedDate']");
            parsedObject.setOriginalId(Collections.singletonList(VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='recordIdentifier']")));

            if (StringUtils.isNotBlank(resolvedDate)) {
                StructuredProperty currentDate = new StructuredProperty();
                currentDate.setValue(resolvedDate);
                final Qualifier dateQualifier = new Qualifier();
                dateQualifier.setClassname("resolvedDate");
                dateQualifier.setClassid("resolvedDate");
                dateQualifier.setSchemename("dnet::date");
                dateQualifier.setSchemeid("dnet::date");
                currentDate.setQualifier(dateQualifier);
                parsedObject.setRelevantdate(Collections.singletonList(currentDate));
            }


            final List<Node> pid = VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='pid']", Arrays.asList("type"));

            StructuredProperty currentPid = extractIdentifier(pid, "type");
            if (currentPid == null) return null;
            inferPid(currentPid);
            parsedObject.setPid(Collections.singletonList(currentPid));
            final String sourceId = generateId(currentPid.getValue(), currentPid.getQualifier().getClassid(), "publication");
            parsedObject.setId(sourceId);

            parsedObject.setOriginalObjIdentifier(VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='objIdentifier']"));

            String provisionMode = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='provisionMode']");

            List<Node> collectedFromNodes =
                    VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='collectedFrom']", Arrays.asList("name", "id", "mode", "completionStatus"));

            List<Node> resolvededFromNodes =
                    VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='resolvedFrom']", Arrays.asList("name", "id", "mode", "completionStatus"));

            final String publisher = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='publisher']");
            Field<String> pf = new Field<>();
            pf.setValue(publisher);

            parsedObject.setPublisher(pf);
            final List<ProvenaceInfo> provenances = new ArrayList<>();
            if (collectedFromNodes != null && collectedFromNodes.size() > 0) {
                collectedFromNodes.forEach(it -> {
                    final ProvenaceInfo provenance = new ProvenaceInfo();
                    provenance.setId(it.getAttributes().get("id"));
                    provenance.setName(it.getAttributes().get("name"));
                    provenance.setCollectionMode(provisionMode);
                    provenance.setCompletionStatus(it.getAttributes().get("completionStatus"));
                    provenances.add(provenance);
                });
            }

            if (resolvededFromNodes != null && resolvededFromNodes.size() > 0) {
                resolvededFromNodes.forEach(it -> {
                    final ProvenaceInfo provenance = new ProvenaceInfo();
                    provenance.setId(it.getAttributes().get("id"));
                    provenance.setName(it.getAttributes().get("name"));
                    provenance.setCollectionMode("resolved");
                    provenance.setCompletionStatus(it.getAttributes().get("completionStatus"));
                    provenances.add(provenance);
                });
            }

            parsedObject.setDlicollectedfrom(provenances);
            parsedObject.setCompletionStatus(VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='completionStatus']"));

            parsedObject.setCollectedfrom(parsedObject.getDlicollectedfrom().stream().map(
                    p -> {
                        final KeyValue cf = new KeyValue();
                        cf.setKey(p.getId());
                        cf.setValue(p.getName());
                        return cf;
                    }
            ).collect(Collectors.toList()));

            final List<Node> relatedIdentifiers =
                    VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='relatedIdentifier']",
                            Arrays.asList("relatedIdentifierType", "relationType", "entityType", "inverseRelationType"));


            if (relatedIdentifiers != null) {
                result.addAll(relatedIdentifiers.stream()
                        .flatMap(n -> {
                            final List<Relation> rels = new ArrayList<>();
                            Relation r = new Relation();
                            r.setSource(parsedObject.getId());
                            final String relatedPid = n.getTextValue();
                            final String relatedPidType = n.getAttributes().get("relatedIdentifierType");
                            final String relatedType = n.getAttributes().getOrDefault("entityType", "unknown");
                            String relationSemantic = n.getAttributes().get("relationType");
                            String inverseRelation = "Unknown";
                            final String targetId = generateId(relatedPid, relatedPidType, relatedType);

                            if (relationMapper.containsKey(relationSemantic.toLowerCase()))
                            {
                                RelInfo relInfo = relationMapper.get(relationSemantic.toLowerCase());
                                relationSemantic = relInfo.getOriginal();
                                inverseRelation = relInfo.getInverse();
                            }
                            else {
                                relationSemantic = "Unknown";
                            }
                            r.setTarget(targetId);
                            r.setRelType(relationSemantic);
                            r.setCollectedFrom(parsedObject.getCollectedfrom());
                            r.setRelClass("datacite");
                            r.setDataInfo(di);
                            rels.add(r);
                            r = new Relation();
                            r.setDataInfo(di);
                            r.setSource(targetId);
                            r.setTarget(parsedObject.getId());
                            r.setRelType(inverseRelation);
                            r.setRelClass("datacite");
                            r.setCollectedFrom(parsedObject.getCollectedfrom());
                            rels.add(r);

                            return rels.stream();
                        }).collect(Collectors.toList()));
            }

            final List<Node> hostedBy =
                    VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='hostedBy']", Arrays.asList("id", "name"));


            if (hostedBy != null) {
                parsedObject.setInstance(hostedBy.stream().map(it ->
                {
                    final Instance i = new Instance();
                    i.setUrl(Collections.singletonList(currentPid.getValue()));
                    KeyValue h = new KeyValue();
                    i.setHostedby(h);
                    h.setKey(it.getAttributes().get("id"));
                    h.setValue(it.getAttributes().get("name"));
                    return i;
                }).collect(Collectors.toList()));
            }

            final List<String> authorsNode = VtdUtilityParser.getTextValue(ap, vn, "//*[local-name()='creator']");
            if (authorsNode != null)
                parsedObject.setAuthor(authorsNode
                        .stream()
                        .map(a -> {
                            final Author author = new Author();
                            author.setFullname(a);
                            return author;
                        }).collect(Collectors.toList())
                );

            final List<String> titles = VtdUtilityParser.getTextValue(ap, vn, "//*[local-name()='title']");
            if (titles != null) {
                parsedObject.setTitle(titles.stream()
                        .map(t -> {
                                    final StructuredProperty st = new StructuredProperty();
                                    st.setValue(t);
                                    return st;
                                }
                        ).collect(Collectors.toList())
                );
            }


            Field<String> description = new Field<>();

            description.setValue(VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='description']"));

            if (StringUtils.isNotBlank(description.getValue()) && description.getValue().length() > 512) {
                description.setValue(description.getValue().substring(0, 512));
            }

            parsedObject.setDescription(Collections.singletonList(description));


            final String cd = VtdUtilityParser.getSingleValue(ap, vn, "//*[local-name()='date']");

            StructuredProperty date = new StructuredProperty();
            date.setValue(cd);
            final Qualifier dq = new Qualifier();
            dq.setClassname("date");
            dq.setClassid("date");
            dq.setSchemename("dnet::date");
            dq.setSchemeid("dnet::date");
            date.setQualifier(dq);
            parsedObject.setRelevantdate(Collections.singletonList(date));

            List<StructuredProperty> subjects = extractSubject(VtdUtilityParser.getTextValuesWithAttributes(ap, vn, "//*[local-name()='subject']", Collections.singletonList("scheme")));
            parsedObject.setSubject(subjects);

            parsedObject.setDataInfo(di);

            parsedObject.setSubject(subjects);
            Qualifier q = new Qualifier();
            q.setClassname("publication");
            q.setClassid("publication");
            q.setSchemename("publication");
            q.setSchemeid("publication");
            parsedObject.setResulttype(q);
            result.add(parsedObject);
            return result;

        } catch (Throwable e) {
            log.error("Input record: " + record);
            log.error("Error on parsing record ", e);
            return null;
        }

    }


}
