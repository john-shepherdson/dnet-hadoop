package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import net.minidev.json.JSONArray;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class MappingUtils {

    public EntityRelEntity pruneModel(EntityRelEntity e) throws JsonProcessingException {

        final DocumentContext j = JsonPath.parse(e.getSource().getOaf());
        final RelatedEntity re = new RelatedEntity();

        switch (e.getSource().getType()) {
            case "publication":
            case "dataset":
            case "otherresearchproduct":
            case "software":

                mapTitle(j, re);
                re.setDateofacceptance(j.read("$.dateofacceptance.value"));
                re.setPublisher(j.read("$.publisher.value"));

                JSONArray pids = j.read("$.pid");
                re.setPid(pids.stream()
                        .map(p -> asStructuredProperty((LinkedHashMap<String, Object>) p))
                        .collect(Collectors.toList()));

                re.setResulttype(asQualifier(j.read("$.resulttype")));

                JSONArray collfrom = j.read("$.collectedfrom");
                re.setCollectedfrom(collfrom.stream()
                        .map(c -> asKV((LinkedHashMap<String, Object>)c))
                        .collect(Collectors.toList()));

                //TODO still to be mapped
                //re.setCodeRepositoryUrl(j.read("$.coderepositoryurl"));

                break;
            case "datasource":
                re.setOfficialname(j.read("$.officialname.value"));
                re.setWebsiteurl(j.read("$.websiteurl.value"));

                re.setDatasourcetype(asQualifier(j.read("$.datasourcetype")));
                re.setOpenairecompatibility(asQualifier(j.read("$.openairecompatibility")));

                break;
            case "organization":

                break;
            case "project":
                mapTitle(j, re);
                break;
        }

        return new EntityRelEntity().setSource(
                new TypedRow()
                        .setSourceId(e.getSource().getSourceId())
                        .setDeleted(e.getSource().getDeleted())
                        .setType(e.getSource().getType())
                        .setOaf(new ObjectMapper().writeValueAsString(re)));
    }

    private KeyValue asKV(LinkedHashMap<String, Object> j) {
        final KeyValue kv = new KeyValue();
        kv.setKey((String) j.get("key"));
        kv.setValue((String) j.get("value"));
        return kv;
    }

    private void mapTitle(DocumentContext j, RelatedEntity re) {
        JSONArray a = j.read("$.title");
        if (!a.isEmpty()) {
            re.setTitle(asStructuredProperty((LinkedHashMap<String, Object>) a.get(0)));
        }
    }

    private StructuredProperty asStructuredProperty(LinkedHashMap<String, Object> j) {
        final StructuredProperty sp = new StructuredProperty();
        sp.setValue((String) j.get("value"));
        sp.setQualifier(asQualifier((LinkedHashMap<String, String>) j.get("qualifier")));
        return sp;

    }

    public Qualifier asQualifier(LinkedHashMap<String, String> j) {
        Qualifier q = new Qualifier();
        q.setClassid(j.get("classid"));
        q.setClassname(j.get("classname"));
        q.setSchemeid(j.get("schemeid"));
        q.setSchemename(j.get("schemename"));
        return q;
    }

}
