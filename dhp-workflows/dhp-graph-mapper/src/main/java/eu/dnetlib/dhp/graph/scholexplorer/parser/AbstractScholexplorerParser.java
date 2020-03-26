package eu.dnetlib.dhp.graph.scholexplorer.parser;


import eu.dnetlib.dhp.parser.utility.VtdUtilityParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.stream.XMLStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractScholexplorerParser {

    protected static final Log log = LogFactory.getLog(AbstractScholexplorerParser.class);
    final static Pattern pattern = Pattern.compile("10\\.\\d{4,9}/[-._;()/:A-Z0-9]+$", Pattern.CASE_INSENSITIVE);
    private List<String> datasetSubTypes = Arrays.asList("dataset", "software", "film", "sound", "physicalobject", "audiovisual", "collection", "other", "study", "metadata");

    public abstract List<Oaf> parseObject(final String record, final RelationMapper relMapper);

    protected Map<String, String> getAttributes(final XMLStreamReader parser) {
        final Map<String, String> attributesMap = new HashMap<>();
        for (int i = 0; i < parser.getAttributeCount(); i++) {
            attributesMap.put(parser.getAttributeLocalName(i), parser.getAttributeValue(i));
        }
        return attributesMap;
    }


    protected List<StructuredProperty> extractSubject(List<VtdUtilityParser.Node> subjects) {
        final List<StructuredProperty> subjectResult = new ArrayList<>();
        if (subjects != null && subjects.size() > 0) {
            subjects.forEach(subjectMap -> {
                final StructuredProperty subject = new StructuredProperty();
                subject.setValue(subjectMap.getTextValue());
                final Qualifier schema = new Qualifier();
                schema.setClassid("dnet:subject");
                schema.setClassname("dnet:subject");
                schema.setSchemeid(subjectMap.getAttributes().get("subjectScheme"));
                schema.setSchemename(subjectMap.getAttributes().get("subjectScheme"));
                subject.setQualifier(schema);
                subjectResult.add(subject);
            });
        }
        return subjectResult;
    }


    protected StructuredProperty extractIdentifier(List<VtdUtilityParser.Node> identifierType, final String fieldName) {
        final StructuredProperty pid = new StructuredProperty();
        if (identifierType != null && identifierType.size() > 0) {
            final VtdUtilityParser.Node result = identifierType.get(0);
            pid.setValue(result.getTextValue());
            final Qualifier pidType = new Qualifier();
            pidType.setClassname(result.getAttributes().get(fieldName));
            pidType.setClassid(result.getAttributes().get(fieldName));
            pidType.setSchemename("dnet:pid_types");
            pidType.setSchemeid("dnet:pid_types");
            pid.setQualifier(pidType);
            return pid;
        }
        return null;
    }

    protected void inferPid(final StructuredProperty input) {
        final Matcher matcher = pattern.matcher(input.getValue());
        if (matcher.find()) {
            input.setValue(matcher.group());
            if (input.getQualifier() == null) {
                input.setQualifier(new Qualifier());
                input.getQualifier().setSchemename("dnet:pid_types");
                input.getQualifier().setSchemeid("dnet:pid_types");
            }
            input.getQualifier().setClassid("doi");
            input.getQualifier().setClassname("doi");
        }
    }

    protected String generateId(final String pid, final String pidType, final String entityType) {
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
        if ("dnet".equalsIgnoreCase(pidType))
            return type+StringUtils.substringAfter(pid, "::");

        return type+ DHPUtils.md5(String.format("%s::%s", pid.toLowerCase().trim(), pidType.toLowerCase().trim()));
    }




}



