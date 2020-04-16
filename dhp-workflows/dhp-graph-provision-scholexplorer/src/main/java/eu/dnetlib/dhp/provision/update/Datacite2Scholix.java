package eu.dnetlib.dhp.provision.update;

import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.provision.scholix.*;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.scholexplorer.relation.RelInfo;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Datacite2Scholix {

    private String rootPath = "$.attributes";
    final RelationMapper relationMapper;

    public Datacite2Scholix(RelationMapper relationMapper) {
        this.relationMapper = relationMapper;
    }

    public List<Scholix> generateScholixFromJson(final String dJson) {
        List<Map<String, String>> relIds = getRelatedIendtifiers(dJson);
        relIds = relIds!= null ? relIds.stream().filter(m->
                m.containsKey("relatedIdentifierType") && m.containsKey("relationType" ) && m.containsKey( "relatedIdentifier")
        ).collect(Collectors.toList()) : null;
        if(relIds== null || relIds.size() ==0 )
            return null;

        final String updated = JsonPath.read(dJson, rootPath + ".updated");
        ScholixResource resource = generateDataciteScholixResource(dJson);

        return relIds.stream().flatMap(s-> {
            final List<Scholix> result = generateScholix(resource, s.get("relatedIdentifier"), s.get("relatedIdentifierType"), s.get("relationType"), updated);
            return result.stream();
        }).collect(Collectors.toList());
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    private List<Scholix> generateScholix(ScholixResource source, final String pid, final String pidtype, final String relType, final String updated) {
        if ("doi".equalsIgnoreCase(pidtype)) {
            ScholixResource target = new ScholixResource();
            target.setIdentifier(Collections.singletonList(new ScholixIdentifier(pid, pidtype)));
            final RelInfo relInfo = relationMapper.get(relType.toLowerCase());
            final ScholixRelationship rel = new ScholixRelationship(relInfo.getOriginal(), "datacite", relInfo.getInverse());
            final ScholixEntityId provider = source.getCollectedFrom().get(0).getProvider();
            final Scholix s = new Scholix();
            s.setSource(source);
            s.setTarget(target);
            s.setLinkprovider(Collections.singletonList(provider));
            s.setPublisher(source.getPublisher());
            s.setRelationship(rel);
            s.setPublicationDate(updated);
            return Collections.singletonList(s);
        } else {
            final List<Scholix> result = new ArrayList<>();
            ScholixResource target = new ScholixResource();
            target.setIdentifier(Collections.singletonList(new ScholixIdentifier(pid, pidtype)));
            target.setDnetIdentifier(generateId(pid, pidtype, "unknown"));
            target.setObjectType("unknown");
            target.setCollectedFrom(generateDataciteCollectedFrom("incomplete"));
            final RelInfo relInfo = relationMapper.get(relType.toLowerCase());
            final ScholixRelationship rel = new ScholixRelationship(relInfo.getOriginal(), "datacite", relInfo.getInverse());
            final ScholixEntityId provider = source.getCollectedFrom().get(0).getProvider();
            final Scholix s = new Scholix();
            s.setSource(source);
            s.setTarget(target);
            s.setLinkprovider(Collections.singletonList(provider));
            s.setPublisher(source.getPublisher());
            s.setRelationship(rel);
            s.setPublicationDate(updated);
            s.generateIdentifier();
            result.add(s);
            final Scholix s2 = new Scholix();
            s2.setSource(target);
            s2.setTarget(source);
            s2.setLinkprovider(Collections.singletonList(provider));
            s2.setPublisher(source.getPublisher());
            s2.setRelationship(new ScholixRelationship(relInfo.getInverse(), "datacite", relInfo.getOriginal()));
            s2.setPublicationDate(updated);
            s2.generateIdentifier();
            result.add(s2);
            return result;
        }
    }

    public ScholixResource generateDataciteScholixResource(String dJson) {
        ScholixResource resource = new ScholixResource();
        String DOI_PATH = rootPath + ".doi";
        final String doi = JsonPath.read(dJson, DOI_PATH);
        resource.setIdentifier(Collections.singletonList(new ScholixIdentifier(doi, "doi")));
        resource.setObjectType(getType(dJson));
        resource.setDnetIdentifier(generateId(doi, "doi", resource.getObjectType()));
        resource.setCollectedFrom(generateDataciteCollectedFrom("complete"));
        final String publisher = JsonPath.read(dJson, rootPath + ".publisher");
        if (StringUtils.isNotBlank(publisher))
            resource.setPublisher(Collections.singletonList(new ScholixEntityId(publisher, null)));
        final String date = getDate(dJson);
        if (StringUtils.isNotBlank(date))
            resource.setPublicationDate(date);
        final String title = getTitle(dJson);
        if(StringUtils.isNotBlank(title))
            resource.setTitle(title);
        resource.setCreator(getCreators(dJson));
        return resource;
    }

    private List<ScholixEntityId> getCreators(final String json) {
        final List<String> creatorName = JsonPath.read(json, rootPath + ".creators[*].name");
        if (creatorName!= null && creatorName.size() >0) {
            return  creatorName.stream().map(s-> new ScholixEntityId(s, null)).collect(Collectors.toList());
        }
        return null;
    }

    private String getTitle(final String json){
        final List<String> titles = JsonPath.read(json, rootPath + ".titles[*].title");
        return titles!= null && titles.size()>0?titles.get(0): null;
    }

    private String getDate(final String json) {
        final  List<Map<String,String>> dates = JsonPath.read(json, rootPath + ".dates");
        if(dates!= null && dates.size()>0){

            List<Map<String, String>> issued = dates.stream().filter(s -> "issued".equalsIgnoreCase(s.get("dateType"))).collect(Collectors.toList());
            if (issued.size()>0)
                return issued.get(0).get("date");
        }
        return null;
    }

    private List<ScholixCollectedFrom> generateDataciteCollectedFrom(final String completionStatus) {
        final ScholixEntityId scholixEntityId = new ScholixEntityId("Datasets in Datacite",
                Collections.singletonList(new ScholixIdentifier("dli_________::datacite", "dnet_identifier")));
        return Collections.singletonList(
                new ScholixCollectedFrom(
                        scholixEntityId,"collected", completionStatus));
    }

    private String getType(final String json) {
        try {
            final String bibtext = JsonPath.read(json, rootPath + ".types.bibtex");
            if ("article".equalsIgnoreCase(bibtext)) {
                return "publication";
            }
            return "dataset";
        } catch (Throwable e) {
            return "dataset";
        }
    }

    private List<Map<String, String>> getRelatedIendtifiers(final String json) {
        String REL_IDENTIFIER_PATH = rootPath + ".relatedIdentifiers[*]";
        List<Map<String, String>> res = JsonPath.read(json, REL_IDENTIFIER_PATH);
        return res;
    }

    public static String generateId(final String pid, final String pidType, final String entityType) {
        String type;
        switch (entityType){
            case "publication":
                type = "50|";
                break;
            case "dataset":
                type = "60|";
                break;
            case "unknown":
                type = "70|";
                break;
            default:
                throw new IllegalArgumentException("unexpected value "+entityType);
        }
        return type+ DHPUtils.md5(String.format("%s::%s", pid.toLowerCase().trim(), pidType.toLowerCase().trim()));
    }
}
