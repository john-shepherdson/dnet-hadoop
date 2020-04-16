package eu.dnetlib.dhp.provision.update;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.dnetlib.dhp.provision.scholix.ScholixCollectedFrom;
import eu.dnetlib.dhp.provision.scholix.ScholixEntityId;
import eu.dnetlib.dhp.provision.scholix.ScholixIdentifier;
import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import eu.dnetlib.dhp.utils.DHPUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CrossRefParserJSON {

    private static List<ScholixCollectedFrom> collectedFrom =generateCrossrefCollectedFrom("complete");

    public static ScholixResource parseRecord(final String record) {
        if (record == null) return null;
        JsonElement jElement = new JsonParser().parse(record);
        JsonElement source = null;
        if (jElement.getAsJsonObject().has("_source")) {
            source = jElement.getAsJsonObject().get("_source");
            if (source == null || !source.isJsonObject())
                return null;
        }
        else if(jElement.getAsJsonObject().has("DOI")){
            source = jElement;
        } else {
            return null;
        }

        final JsonObject message = source.getAsJsonObject();
        ScholixResource currentObject = new ScholixResource();

        if (message.get("DOI") != null) {
            final String doi = message.get("DOI").getAsString();
            currentObject.setIdentifier(Collections.singletonList(new ScholixIdentifier(doi, "doi")));
        }

        if ((!message.get("created").isJsonNull()) && (message.getAsJsonObject("created").get("date-time") != null)) {
            currentObject.setPublicationDate(message.getAsJsonObject("created").get("date-time").getAsString());
        }

        if (message.get("title")!= null && !message.get("title").isJsonNull() && message.get("title").isJsonArray() ) {

            JsonArray array = message.get("title").getAsJsonArray();
            currentObject.setTitle(array.get(0).getAsString());
        }
        if (message.get("author") != null && !message.get("author").isJsonNull()) {
            JsonArray author = message.getAsJsonArray("author");
            List<ScholixEntityId> authorList = new ArrayList<>();
            for (JsonElement anAuthor : author) {
                JsonObject currentAuth = anAuthor.getAsJsonObject();

                String family = "";
                String given = "";
                if (currentAuth != null && currentAuth.get("family") != null && !currentAuth.get("family").isJsonNull()) {
                    family = currentAuth.get("family").getAsString();
                }
                if (currentAuth != null && currentAuth.get("given") != null && !currentAuth.get("given").isJsonNull()) {
                    given = currentAuth.get("given").getAsString();
                }
                authorList.add(new ScholixEntityId(String.format("%s %s", family, given), null));
            }
            currentObject.setCreator(authorList);
        }
        if (message.get("publisher") != null && !message.get("publisher").isJsonNull()) {
            currentObject.setPublisher(Collections.singletonList(new ScholixEntityId(message.get("publisher").getAsString(), null)));
        }
        currentObject.setCollectedFrom(collectedFrom);
        currentObject.setObjectType("publication");
        currentObject.setDnetIdentifier(generateId(message.get("DOI").getAsString(), "doi", "publication"));

        return currentObject;
    }

    private static List<ScholixCollectedFrom> generateCrossrefCollectedFrom(final String completionStatus) {
        final ScholixEntityId scholixEntityId = new ScholixEntityId("Crossref",
                Collections.singletonList(new ScholixIdentifier("dli_________::crossref", "dnet_identifier")));
        return Collections.singletonList(
                new ScholixCollectedFrom(
                        scholixEntityId,"resolved", completionStatus));
    }

    private static String generateId(final String pid, final String pidType, final String entityType) {
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
