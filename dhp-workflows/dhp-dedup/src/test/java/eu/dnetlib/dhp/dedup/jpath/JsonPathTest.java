package eu.dnetlib.dhp.dedup.jpath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.util.List;
import java.util.Map;

public class JsonPathTest {

    String json = "{\t\"dataInfo\":{\t\t\"invisible\":false,\t\t\"inferred\":false,\t\t\"deletedbyinference\":false,\t\t\"trust\":\"0.810000002384185791\",\t\t\"inferenceprovenance\":\"\",\t\t\"provenanceaction\":{\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t}\t},\t\"lastupdatetimestamp\":1584960968152,\t\"id\":\"20|corda__h2020::9faf23721249f26ac2c16eb857ea1fb9\",\t\"originalId\":[\t\t\"corda__h2020::927957582\"\t],\t\"collectedfrom\":[\t\t{\t\t\t\"key\":\"openaire____::corda_h2020\",\t\t\t\"value\":\"CORDA - COmmon Research DAta Warehouse - Horizon 2020\",\t\t\t\"dataInfo\":null\t\t}\t],\t\"pid\":[\t],\t\"dateofcollection\":\"2016-06-05\",\t\"dateoftransformation\":\"2019-11-19\",\t\"extraInfo\":[\t],\t\"oaiprovenance\":null,\t\"legalshortname\":{\t\t\"value\":\"Comentor AB\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"legalname\":{\t\t\"value\":\"Comentor AB\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"alternativeNames\":[\t],\t\"websiteurl\":{\t\t\"value\":\"http://www.comentor.se\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"logourl\":null,\t\"eclegalbody\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"eclegalperson\":{\t\t\"value\":\"true\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecnonprofit\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecresearchorganization\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"echighereducation\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecinternationalorganizationeurinterests\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecinternationalorganization\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecenterprise\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecsmevalidated\":{\t\t\"value\":\"true\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"ecnutscode\":{\t\t\"value\":\"false\",\t\t\"dataInfo\":{\t\t\t\"invisible\":false,\t\t\t\"inferred\":false,\t\t\t\"deletedbyinference\":false,\t\t\t\"trust\":\"0.810000002384185791\",\t\t\t\"inferenceprovenance\":\"\",\t\t\t\"provenanceaction\":{\t\t\t\t\"classid\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"classname\":\"sysimport:crosswalk:entityregistry\",\t\t\t\t\"schemeid\":\"dnet:provenance_actions\",\t\t\t\t\"schemename\":\"dnet:provenance_actions\"\t\t\t}\t\t}\t},\t\"country\":null}";
    DedupConfig conf = DedupConfig.load("{\n" +
            "  \"wf\" : {\n" +
            "    \"threshold\" : \"0.99\",\n" +
            "    \"dedupRun\" : \"001\",\n" +
            "    \"entityType\" : \"organization\",\n" +
            "    \"subEntityValue\": \"organization\",\n" +
            "    \"orderField\" : \"legalname\",\n" +
            "    \"queueMaxSize\" : \"2000\",\n" +
            "    \"groupMaxSize\" : \"50\",\n" +
            "    \"slidingWindowSize\" : \"200\",\n" +
            "    \"idPath\":\"$.id\",\n" +
            "    \"rootBuilder\" : [ \"organization\", \"projectOrganization_participation_isParticipant\", \"datasourceOrganization_provision_isProvidedBy\" ],\n" +
            "    \"includeChildren\" : \"true\",\n" +
            "    \"maxIterations\": \"20\"\n" +
            "  },\n" +
            "  \"pace\" : {\n" +
            "    \"clustering\" : [\n" +
            "      { \"name\" : \"sortedngrampairs\", \"fields\" : [ \"legalname\" ], \"params\" : { \"max\" : 2, \"ngramLen\" : \"3\"} },\n" +
            "      { \"name\" : \"suffixprefix\", \"fields\" : [ \"legalname\" ], \"params\" : { \"max\" : 1, \"len\" : \"3\" } },\n" +
            "      { \"name\" : \"urlclustering\", \"fields\" : [ \"websiteurl\" ], \"params\" : { } },\n" +
            "      { \"name\" : \"keywordsclustering\", \"fields\" : [ \"legalname\" ], \"params\" : { \"max\": 2, \"windowSize\": 4} }\n" +
            "    ],\n" +
            "    \"decisionTree\" : {\n" +
            "      \"start\": {\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"field\": \"gridid\",\n" +
            "            \"comparator\": \"exactMatch\",\n" +
            "            \"weight\": 1,\n" +
            "            \"countIfUndefined\": \"false\",\n" +
            "            \"params\": {}\n" +
            "          }\n" +
            "        ],\n" +
            "        \"threshold\": 1,\n" +
            "        \"aggregation\": \"AVG\",\n" +
            "        \"positive\": \"MATCH\",\n" +
            "        \"negative\": \"NO_MATCH\",\n" +
            "        \"undefined\": \"layer2\",\n" +
            "        \"ignoreUndefined\": \"false\"\n" +
            "      },\n" +
            "      \"layer2\": {\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"field\": \"websiteurl\",\n" +
            "            \"comparator\": \"domainExactMatch\",\n" +
            "            \"weight\": 1,\n" +
            "            \"countIfUndefined\": \"false\",\n" +
            "            \"params\": {}\n" +
            "          },\n" +
            "          {\n" +
            "            \"field\": \"country\",\n" +
            "            \"comparator\": \"exactMatch\",\n" +
            "            \"weight\": 1,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {}\n" +
            "          },\n" +
            "          {\n" +
            "            \"field\": \"legalname\",\n" +
            "            \"comparator\": \"numbersMatch\",\n" +
            "            \"weight\": 1,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {}\n" +
            "          },\n" +
            "          {\n" +
            "            \"field\": \"legalname\",\n" +
            "            \"comparator\": \"romansMatch\",\n" +
            "            \"weight\": 1,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {}\n" +
            "          }\n" +
            "        ],\n" +
            "        \"threshold\": 1,\n" +
            "        \"aggregation\": \"AND\",\n" +
            "        \"positive\": \"layer3\",\n" +
            "        \"negative\": \"NO_MATCH\",\n" +
            "        \"undefined\": \"layer3\",\n" +
            "        \"ignoreUndefined\": \"true\"\n" +
            "      },\n" +
            "      \"layer3\": {\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"field\": \"legalname\",\n" +
            "            \"comparator\": \"cityMatch\",\n" +
            "            \"weight\": 1.0,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {\n" +
            "              \"windowSize\": \"4\"\n" +
            "            }\n" +
            "          }\n" +
            "        ],\n" +
            "        \"threshold\": 0.1,\n" +
            "        \"aggregation\": \"AVG\",\n" +
            "        \"positive\": \"layer4\",\n" +
            "        \"negative\": \"NO_MATCH\",\n" +
            "        \"undefined\": \"NO_MATCH\",\n" +
            "        \"ignoreUndefined\": \"true\"\n" +
            "      },\n" +
            "      \"layer4\": {\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"field\": \"legalname\",\n" +
            "            \"comparator\": \"keywordMatch\",\n" +
            "            \"weight\": 1.0,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {\n" +
            "              \"windowSize\": \"4\"\n" +
            "            }\n" +
            "          }\n" +
            "        ],\n" +
            "        \"threshold\": 0.7,\n" +
            "        \"aggregation\": \"AVG\",\n" +
            "        \"positive\": \"layer5\",\n" +
            "        \"negative\": \"NO_MATCH\",\n" +
            "        \"undefined\": \"layer5\",\n" +
            "        \"ignoreUndefined\": \"true\"\n" +
            "      },\n" +
            "      \"layer5\": {\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"field\": \"legalname\",\n" +
            "            \"comparator\": \"jaroWinklerNormalizedName\",\n" +
            "            \"weight\": 0.9,\n" +
            "            \"countIfUndefined\": \"true\",\n" +
            "            \"params\": {\n" +
            "              \"windowSize\": \"4\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"field\": \"legalshortname\",\n" +
            "            \"comparator\": \"jaroWinklerNormalizedName\",\n" +
            "            \"weight\": 0.1,\n" +
            "            \"countIfUndefined\": \"false\",\n" +
            "            \"params\": {\n" +
            "              \"windowSize\": 4\n" +
            "            }\n" +
            "          }\n" +
            "        ],\n" +
            "        \"threshold\": 0.9,\n" +
            "        \"aggregation\": \"W_MEAN\",\n" +
            "        \"positive\": \"MATCH\",\n" +
            "        \"negative\": \"NO_MATCH\",\n" +
            "        \"undefined\": \"NO_MATCH\",\n" +
            "        \"ignoreUndefined\": \"true\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"model\" : [\n" +
            "      { \"name\" : \"country\", \"type\" : \"String\", \"path\" : \"$.country.classid\"},\n" +
            "      { \"name\" : \"legalshortname\", \"type\" : \"String\", \"path\" : \"$.legalshortname.value\"},\n" +
            "      { \"name\" : \"legalname\", \"type\" : \"String\", \"path\" : \"$.legalname.value\" },\n" +
            "      { \"name\" : \"websiteurl\", \"type\" : \"URL\", \"path\" : \"$.websiteurl.value\" },\n" +
            "      { \"name\" : \"gridid\", \"type\" : \"String\", \"path\" : \"$.pid[?(@.qualifier.classid =='grid')].value\"},\n" +
            "      { \"name\" : \"originalId\", \"type\" : \"String\", \"path\" : \"$.id\" }\n" +
            "    ],\n" +
            "    \"blacklists\" : {\n" +
            "      \"legalname\" : []\n" +
            "    },\n" +
            "    \"synonyms\": {\n" +
            "      \"key::1\": [\"university\",\"università\", \"universitas\", \"università studi\",\"universitario\",\"universitaria\",\"université\", \"universite\", \"universitaire\",\"universitaires\",\"universidad\",\"universitade\",\"Universität\",\"universitaet\",\"Uniwersytet\",\"университет\",\"universiteit\",\"πανεπιστήμιο\",\"universitesi\",\"universiteti\", \"universiti\"],\n" +
            "      \"key::2\": [\"studies\",\"studi\",\"études\",\"estudios\",\"estudos\",\"Studien\",\"studia\",\"исследования\",\"studies\",\"σπουδές\"],\n" +
            "      \"key::3\": [\"advanced\",\"superiore\",\"supérieur\",\"supérieure\",\"supérieurs\",\"supérieures\",\"avancado\",\"avancados\",\"fortgeschrittene\",\"fortgeschritten\",\"zaawansowany\",\"передовой\",\"gevorderd\",\"gevorderde\",\"προχωρημένος\",\"προχωρημένη\",\"προχωρημένο\",\"προχωρημένες\",\"προχωρημένα\",\"wyzsza\"],\n" +
            "      \"key::4\": [\"institute\",\"istituto\",\"institut\",\"instituto\",\"instituto\",\"Institut\",\"instytut\",\"институт\",\"instituut\",\"ινστιτούτο\"],\n" +
            "      \"key::5\": [\"hospital\",\"ospedale\",\"hôpital\",\"hospital\",\"hospital\",\"Krankenhaus\",\"szpital\",\"больница\",\"ziekenhuis\",\"νοσοκομείο\"],\n" +
            "      \"key::6\": [\"research\",\"ricerca\",\"recherche\",\"investigacion\",\"pesquisa\",\"Forschung\",\"badania\",\"исследования\",\"onderzoek\",\"έρευνα\",\"erevna\",\"erevnas\"],\n" +
            "      \"key::7\": [\"college\",\"collegio\",\"colegio\",\"faculdade\",\"Hochschule\",\"Szkoła Wyższa\",\"Высшая школа\",\"κολλέγιο\"],\n" +
            "      \"key::8\": [\"foundation\",\"fondazione\",\"fondation\",\"fundación\",\"fundação\",\"Stiftung\",\"Fundacja\",\"фонд\",\"stichting\",\"ίδρυμα\",\"idryma\"],\n" +
            "      \"key::9\": [\"center\",\"centro\",\"centre\",\"centro\",\"centro\",\"zentrum\",\"centrum\",\"центр\",\"centrum\",\"κέντρο\"],\n" +
            "      \"key::10\": [\"national\",\"nazionale\",\"national\",\"nationale\",\"nationaux\",\"nationales\",\"nacional\",\"nacional\",\"national\",\"krajowy\",\"национальный\",\"nationaal\",\"nationale\",\"εθνικό\"],\n" +
            "      \"key::11\": [\"association\",\"associazione\",\"association\",\"asociación\",\"associação\",\"Verein\",\"verband\",\"stowarzyszenie\",\"ассоциация\",\"associatie\"],\n" +
            "      \"key::12\": [\"society\",\"societa\",\"société\",\"sociedad\",\"sociedade\",\"gesellschaft\",\"społeczeństwo\",\"общество\",\"maatschappij\",\"κοινωνία\"],\n" +
            "      \"key::13\": [\"international\",\"internazionale\",\"international\",\"internacional\",\"internacional\",\"international\",\"międzynarodowy\",\"Международный\",\"internationaal\",\"internationale\",\"διεθνής\",\"διεθνή\",\"διεθνές\"],\n" +
            "      \"key::14\": [\"community\",\"comunita\",\"communauté\",\"comunidad\",\"comunidade\",\"Gemeinschaft\",\"społeczność\",\"сообщество\",\"gemeenschap\",\"κοινότητα\"],\n" +
            "      \"key::15\": [\"school\",\"scuola\",\"école\",\"escuela\",\"escola\",\"schule\",\"Szkoła\",\"школа\",\"school\",\"σχολείο\"],\n" +
            "      \"key::16\": [\"education\",\"educazione\",\"éducation\",\"educacion\",\"Educação\",\"Bildung\",\"Edukacja\",\"образование\",\"opleiding\",\"εκπαίδευση\"],\n" +
            "      \"key::17\": [\"academy\",\"accademia\",\"académie\",\"academia\",\"academia\",\"Akademie\",\"akademie\",\"академия\",\"academie\",\"ακαδημία\"],\n" +
            "      \"key::18\": [\"public\",\"pubblico\",\"public\",\"publique\",\"publics\",\"publiques\",\"publico\",\"publico\",\"Öffentlichkeit\",\"publiczny\",\"публичный\",\"publiek\",\"publieke\",\"δημόσιος\",\"δημόσια\",\"δημόσιο\"],\n" +
            "      \"key::19\": [\"museum\",\"museo\",\"musée\",\"mueso\",\"museu\",\"museum\",\"muzeum\",\"музей\",\"museum\",\"μουσείο\"],\n" +
            "      \"key::20\": [\"group\",\"gruppo\",\"groupe\",\"grupo\",\"grupo\",\"gruppe\",\"grupa\",\"группа\",\"groep\",\"ομάδα\",\"όμιλος\"],\n" +
            "      \"key::21\": [\"department\",\"dipartimento\",\"département\",\"departamento\",\"departamento\",\"abteilung\",\"departament\",\"отдел\",\"afdeling\",\"τμήμα\"],\n" +
            "      \"key::22\": [\"council\",\"consiglio\",\"conseil\",\"Consejo\",\"conselho\",\"gesellschaft\",\"rada\",\"совет\",\"raad\",\"συμβούλιο\"],\n" +
            "      \"key::23\": [\"library\",\"biblioteca\",\"bibliothèque\",\"biblioteca\",\"biblioteca\",\"Bibliothek\",\"biblioteka\",\"библиотека\",\"bibliotheek\",\"βιβλιοθήκη\"],\n" +
            "      \"key::24\": [\"ministry\",\"ministero\",\"ministère\",\"ministerio\",\"ministério\",\"Ministerium\",\"ministerstwo\",\"министерство\",\"ministerie\",\"υπουργείο\"],\n" +
            "      \"key::25\": [\"services\",\"servizi\",\"services\",\"servicios\",\"Serviços\",\"Dienstleistungen\",\"usługi\",\"услуги\",\"diensten\",\"υπηρεσίες\"],\n" +
            "      \"key::26\": [\"central\",\"centrale\",\"central\",\"centrale\",\"centrales\",\"central\",\"central\",\"zentral\",\"centralny\",\"цетральный\",\"centraal\",\"κεντρικός\",\"κεντρική\",\"κεντρικό\",\"κεντρικά\"],\n" +
            "      \"key::27\": [\"general\",\"generale\",\"général\",\"générale\",\"généraux\",\"générales\",\"general\",\"geral\",\"general\",\"Allgemeines\",\"general\",\"общий\",\"algemeen\",\"algemene\",\"γενικός\",\"γενική\",\"γενικό\",\"γενικά\"],\n" +
            "      \"key::28\": [\"applied\",\"applicati\",\"appliqué\",\"appliquée\",\"appliqués\",\"appliquées\",\"aplicado\",\"aplicada\",\"angewendet\",\"stosowany\",\"прикладной\",\"toegepast\",\"toegepaste\",\"εφαρμοσμένος\",\"εφαρμοσμένη\",\"εφαρμοσμένο\",\"εφαρμοσμένα\"],\n" +
            "      \"key::29\": [\"european\",\"europee\",\"europea\",\"européen\",\"européenne\",\"européens\",\"européennes\",\"europeo\",\"europeu\",\"europäisch\",\"europejski\",\"европейский\",\"Europees\",\"Europese\",\"ευρωπαϊκός\",\"ευρωπαϊκή\",\"ευρωπαϊκό\",\"ευρωπαϊκά\"],\n" +
            "      \"key::30\": [\"agency\",\"agenzia\",\"agence\",\"agencia\",\"agencia\",\"agentur\",\"agencja\",\"агенция\",\"agentschap\",\"πρακτορείο\"],\n" +
            "      \"key::31\": [\"laboratory\",\"laboratorio\",\"laboratoire\",\"laboratorio\",\"laboratorio\",\"labor\",\"laboratorium\",\"лаборатория\",\"laboratorium\",\"εργαστήριο\"],\n" +
            "      \"key::32\": [\"industry\",\"industria\",\"industrie\",\"индустрия\",\"industrie\",\"βιομηχανία\"],\n" +
            "      \"key::33\": [\"industrial\",\"industriale\",\"industriel\",\"industrielle\",\"industriels\",\"industrielles\",\"индустриальный\",\"industrieel\",\"βιομηχανικός\",\"βιομηχανική\",\"βιομηχανικό\",\"βιομηχανικά\",\"βιομηχανικές\"],\n" +
            "      \"key::34\": [\"consortium\",\"consorzio\",\"consortium\",\"консорциум\",\"consortium\",\"κοινοπραξία\"],\n" +
            "      \"key::35\": [\"organization\",\"organizzazione\",\"organisation\",\"organización\",\"organização\",\"organizacja\",\"организация\",\"organisatie\",\"οργανισμός\"],\n" +
            "      \"key::36\": [\"authority\",\"autorità\",\"autorité\",\"авторитет\",\"autoriteit\"],\n" +
            "      \"key::37\": [\"federation\",\"federazione\",\"fédération\",\"федерация\",\"federatie\",\"ομοσπονδία\"],\n" +
            "      \"key::38\": [\"observatory\",\"osservatorio\",\"observatoire\",\"обсерватория\",\"observatorium\",\"αστεροσκοπείο\"],\n" +
            "      \"key::39\": [\"bureau\",\"ufficio\",\"bureau\",\"офис\",\"bureau\",\"γραφείο\"],\n" +
            "      \"key::40\": [\"company\",\"impresa\",\"compagnie\",\"société\",\"компания\",\"bedrijf\",\"εταιρία\"],\n" +
            "      \"key::41\": [\"polytechnic\",\"politecnico\",\"polytechnique\",\"политехника\",\"polytechnisch\",\"πολυτεχνείο\",\"universita politecnica\",\"polytechnic university\",\"universidad politecnica\",\"universitat politecnica\",\"politechnika\",\"politechniki\",\"university technology\",\"university science technology\"],\n" +
            "      \"key::42\": [\"coalition\",\"coalizione\",\"coalition\",\"коалиция\",\"coalitie\",\"συνασπισμός\"],\n" +
            "      \"key::43\": [\"initiative\",\"iniziativa\",\"initiative\",\"инициатива\",\"initiatief\",\"πρωτοβουλία\"],\n" +
            "      \"key::44\": [\"academic\",\"accademico\",\"académique\",\"universitaire\",\"акадеческий academisch\",\"ακαδημαϊκός\",\"ακαδημαϊκή\",\"ακαδημαϊκό\",\"ακαδημαϊκές\",\"ακαδημαϊκοί\"],\n" +
            "      \"key::45\": [\"institution\",\"istituzione\",\"institution\",\"институциональный\",\"instelling\",\"ινστιτούτο\"],\n" +
            "      \"key::46\": [\"division\",\"divisione\",\"division\",\"отделение\",\"divisie\",\"τμήμα\"],\n" +
            "      \"key::47\": [\"committee\",\"comitato\",\"comité\",\"комитет\",\"commissie\",\"επιτροπή\"],\n" +
            "      \"key::48\": [\"promotion\",\"promozione\",\"продвижение\",\"proothisis\",\"forderung\"],\n" +
            "      \"key::49\": [\"medical\",\"medicine\",\"clinical\",\"medicina\",\"clinici\",\"médico\",\"medicina\",\"clínica\",\"médico\",\"medicina\",\"clínica\",\"medizinisch\",\"Medizin\",\"klinisch\",\"medisch\",\"geneeskunde\",\"klinisch\",\"ιατρικός\",\"ιατρική\",\"ιατρικό\",\"ιατρικά\",\"κλινικός\",\"κλινική\",\"κλινικό\",\"κλινικά\",\"tıbbi\",\"tıp\",\"klinik\",\"orvosi\",\"orvostudomány\",\"klinikai\",\"zdravniški\",\"medicinski\",\"klinični\",\"meditsiini\",\"kliinik\",\"kliiniline\"],\n" +
            "      \"key::50\": [\"technology\",\"technological\",\"tecnologia\",\"tecnologie\",\"tecnología\",\"tecnológico\",\"tecnologia\",\"tecnológico\",\"Technologie\",\"technologisch\",\"technologie\",\"technologisch\",\"τεχνολογία\",\"τεχνολογικός\",\"τεχνολογική\",\"τεχνολογικό\",\"teknoloji\",\"teknolojik\",\"technológia\",\"technológiai\",\"tehnologija\",\"tehnološki\",\"tehnoloogia\",\"tehnoloogiline\",\"technologii\",\"technical\",\"texniki\",\"teknik\"],\n" +
            "      \"key::51\": [\"science\",\"scientific\",\"scienza\",\"scientifiche\",\"scienze\",\"ciencia\",\"científico\",\"ciência\",\"científico\",\"Wissenschaft\",\"wissenschaftlich\",\"wetenschap\",\"wetenschappelijk\",\"επιστήμη\",\"επιστημονικός\",\"επιστημονική\",\"επιστημονικό\",\"επιστημονικά\",\"bilim\",\"bilimsel\",\"tudomány\",\"tudományos\",\"znanost\",\"znanstveni\",\"teadus\",\"teaduslik\",\"\"],\n" +
            "      \"key::52\": [\"engineering\",\"ingegneria\",\"ingeniería\",\"engenharia\",\"Ingenieurwissenschaft\",\"ingenieurswetenschappen\",\"bouwkunde\",\"μηχανικός\",\"μηχανική\",\"μηχανικό\",\"mühendislik\",\"mérnöki\",\"Inženirstvo\",\"inseneeria\",\"inseneri\",\"\"],\n" +
            "      \"key::53\": [\"management\",\"gestione\",\"gestionale\",\"gestionali\",\"gestión\",\"administración\",\"gestão\",\"administração\",\"Verwaltung\",\"management\",\"διαχείριση\",\"yönetim\",\"menedzsment\",\"vodstvo\",\"upravljanje\",\"management\",\"juhtkond\",\"juhtimine\",\"haldus\",\"\"],\n" +
            "      \"key::54\": [\"energy\",\"energia\",\"energía\",\"energia\",\"Energie\",\"energie\",\"ενέργεια\",\"enerji\",\"energia\",\"energija\",\"energia\",\"\"],\n" +
            "      \"key::55\": [\"agricultural\",\"agriculture\",\"agricoltura\",\"agricole\",\"agrícola\",\"agricultura\",\"agrícola\",\"agricultura\",\"landwirtschaftlich\",\"Landwirtschaft\",\"landbouwkundig\",\"landbouw\",\"αγροτικός\",\"αγροτική\",\"αγροτικό\",\"γεωργικός\",\"γεωργική\",\"γεωργικό\",\"γεωργία\",\"tarımsal\",\"tarım\",\"mezőgazdasági\",\"mezőgazdaság\",\"poljedelski\",\"poljedelstvo\",\"põllumajandus\",\"põllumajanduslik\",\"\"],\n" +
            "      \"key::56\": [\"information\",\"informazione\",\"información\",\"informação\",\"Information\",\"informatie\",\"πληροφορία\",\"bilgi\",\"információ\",\"informacija\",\"informatsioon\",\"informatycznych\",\"\"],\n" +
            "      \"key::57\": [\"social\",\"sociali\",\"social\",\"social\",\"Sozial\",\"sociaal\",\"maatschappelijk\",\"κοινωνικός\",\"κοινωνική\",\"κοινωνικό\",\"κοινωνικά\",\"sosyal\",\"szociális\",\"družbeni\",\"sotsiaal\",\"sotsiaalne\",\"\"],\n" +
            "      \"key::58\": [\"environmental\",\"ambiente\",\"medioambiental\",\"ambiente\",\"medioambiente\",\"meioambiente\",\"Umwelt\",\"milieu\",\"milieuwetenschap\",\"milieukunde\",\"περιβαλλοντικός\",\"περιβαλλοντική\",\"περιβαλλοντικό\",\"περιβαλλοντικά\",\"çevre\",\"környezeti\",\"okoliški\",\"keskonna\",\"\"],\n" +
            "      \"key::59\": [\"business\",\"economia\",\"economiche\",\"economica\",\"negocio\",\"empresa\",\"negócio\",\"Unternehmen\",\"bedrijf\",\"bedrijfskunde\",\"επιχείρηση\",\"iş\",\"üzleti\",\"posel\",\"ettevõte/äri\",\"\"],\n" +
            "      \"key::60\": [\"pharmaceuticals\",\"pharmacy\",\"farmacia\",\"farmaceutica\",\"farmacéutica\",\"farmacia\",\"farmacêutica\",\"farmácia\",\"Pharmazeutika\",\"Arzneimittelkunde\",\"farmaceutica\",\"geneesmiddelen\",\"apotheek\",\"φαρμακευτικός\",\"φαρμακευτική\",\"φαρμακευτικό\",\"φαρμακευτικά\",\"φαρμακείο\",\"ilaç\",\"eczane\",\"gyógyszerészeti\",\"gyógyszertár\",\"farmacevtika\",\"lekarništvo\",\"farmaatsia\",\"farmatseutiline\",\"\"],\n" +
            "      \"key::61\": [\"healthcare\",\"health services\",\"salute\",\"atenciónmédica\",\"cuidadodelasalud\",\"cuidadoscomasaúde\",\"Gesundheitswesen\",\"gezondheidszorg\",\"ιατροφαρμακευτικήπερίθαλψη\",\"sağlıkhizmeti\",\"egészségügy\",\"zdravstvo\",\"tervishoid\",\"tervishoiu\",\"\"],\n" +
            "      \"key::62\": [\"history\",\"storia\",\"historia\",\"história\",\"Geschichte\",\"geschiedenis\",\"geschiedkunde\",\"ιστορία\",\"tarih\",\"történelem\",\"zgodovina\",\"ajalugu\",\"\"],\n" +
            "      \"key::63\": [\"materials\",\"materiali\",\"materia\",\"materiales\",\"materiais\",\"materialen\",\"υλικά\",\"τεκμήρια\",\"malzemeler\",\"anyagok\",\"materiali\",\"materjalid\",\"vahendid\",\"\"],\n" +
            "      \"key::64\": [\"economics\",\"economia\",\"economiche\",\"economica\",\"economía\",\"economia\",\"Wirtschaft\",\"economie\",\"οικονομικά\",\"οικονομικέςεπιστήμες\",\"ekonomi\",\"közgazdaságtan\",\"gospodarstvo\",\"ekonomija\",\"majanduslik\",\"majandus\",\"\"],\n" +
            "      \"key::65\": [\"therapeutics\",\"terapeutica\",\"terapéutica\",\"terapêutica\",\"therapie\",\"θεραπευτική\",\"tedavibilimi\",\"gyógykezelés\",\"terapevtika\",\"terapeutiline\",\"ravi\",\"\"],\n" +
            "      \"key::66\": [\"oncology\",\"oncologia\",\"oncologico\",\"oncología\",\"oncologia\",\"Onkologie\",\"oncologie\",\"ογκολογία\",\"onkoloji\",\"onkológia\",\"onkologija\",\"onkoloogia\",\"\"],\n" +
            "      \"key::67\": [\"natural\",\"naturali\",\"naturale\",\"natural\",\"natural\",\"natürlich\",\"natuurlijk\",\"φυσικός\",\"φυσική\",\"φυσικό\",\"φυσικά\",\"doğal\",\"természetes\",\"naraven\",\"loodus\",\"\"],\n" +
            "      \"key::68\": [\"educational\",\"educazione\",\"pedagogia\",\"educacional\",\"educativo\",\"educacional\",\"pädagogisch\",\"educatief\",\"εκπαιδευτικός\",\"εκπαιδευτική\",\"εκπαιδευτικό\",\"εκπαιδευτικά\",\"eğitimsel\",\"oktatási\",\"izobraževalen\",\"haridus\",\"hariduslik\",\"\"],\n" +
            "      \"key::69\": [\"biomedical\",\"biomedica\",\"biomédico\",\"biomédico\",\"biomedizinisch\",\"biomedisch\",\"βιοιατρικός\",\"βιοιατρική\",\"βιοιατρικό\",\"βιοιατρικά\",\"biyomedikal\",\"orvosbiológiai\",\"biomedicinski\",\"biomeditsiiniline\",\"\"],\n" +
            "      \"key::70\": [\"veterinary\",\"veterinaria\",\"veterinarie\",\"veterinaria\",\"veterinária\",\"tierärtzlich\",\"veterinair\",\"veeartsenijlkunde\",\"κτηνιατρικός\",\"κτηνιατρική\",\"κτηνιατρικό\",\"κτηνιατρικά\",\"veteriner\",\"állatorvosi\",\"veterinar\",\"veterinarski\",\"veterinaaria\",\"\"],\n" +
            "      \"key::71\": [\"chemistry\",\"chimica\",\"química\",\"química\",\"Chemie\",\"chemie\",\"scheikunde\",\"χημεία\",\"kimya\",\"kémia\",\"kemija\",\"keemia\",\"\"],\n" +
            "      \"key::72\": [\"security\",\"sicurezza\",\"seguridad\",\"segurança\",\"Sicherheit\",\"veiligheid\",\"ασφάλεια\",\"güvenlik\",\"biztonsági\",\"varnost\",\"turvalisus\",\"julgeolek\",\"\"],\n" +
            "      \"key::73\": [\"biotechnology\",\"biotecnologia\",\"biotecnologie\",\"biotecnología\",\"biotecnologia\",\"Biotechnologie\",\"biotechnologie\",\"βιοτεχνολογία\",\"biyoteknoloji\",\"biotechnológia\",\"biotehnologija\",\"biotehnoloogia\",\"\"],\n" +
            "      \"key::74\": [\"military\",\"militare\",\"militari\",\"militar\",\"militar\",\"Militär\",\"militair\",\"leger\",\"στρατιωτικός\",\"στρατιωτική\",\"στρατιωτικό\",\"στρατιωτικά\",\"askeri\",\"katonai\",\"vojaški\",\"vojni\",\"militaar\",\"wojskowa\",\"\"],\n" +
            "      \"key::75\": [\"theological\",\"teologia\",\"teologico\",\"teológico\",\"tecnológica\",\"theologisch\",\"theologisch\",\"θεολογικός\",\"θεολογική\",\"θεολογικό\",\"θεολογικά\",\"teolojik\",\"technológiai\",\"teološki\",\"teoloogia\",\"usuteadus\",\"teoloogiline\",\"\"],\n" +
            "      \"key::76\": [\"electronics\",\"elettronica\",\"electrónica\",\"eletrônicos\",\"Elektronik\",\"elektronica\",\"ηλεκτρονική\",\"elektronik\",\"elektronika\",\"elektronika\",\"elektroonika\",\"\"],\n" +
            "      \"key::77\": [\"forestry\",\"forestale\",\"forestali\",\"silvicultura\",\"forestal\",\"floresta\",\"Forstwirtschaft\",\"bosbouw\",\"δασοκομία\",\"δασολογία\",\"ormancılık\",\"erdészet\",\"gozdarstvo\",\"metsandus\",\"\"],\n" +
            "      \"key::78\": [\"maritime\",\"marittima\",\"marittime\",\"marittimo\",\"marítimo\",\"marítimo\",\"maritiem\",\"ναυτικός\",\"ναυτική\",\"ναυτικό\",\"ναυτικά\",\"ναυτιλιακός\",\"ναυτιλιακή\",\"ναυτιλιακό\",\"ναυτιλιακά\",\"θαλάσσιος\",\"θαλάσσια\",\"θαλάσσιο\",\"denizcilik\",\"tengeri\",\"morski\",\"mere\",\"merendus\",\"\"],\n" +
            "      \"key::79\": [\"sports\",\"sport\",\"deportes\",\"esportes\",\"Sport\",\"sport\",\"sportwetenschappen\",\"άθληση\",\"γυμναστικήδραστηριότητα\",\"spor\",\"sport\",\"šport\",\"sport\",\"spordi\",\"\"],\n" +
            "      \"key::80\": [\"surgery\",\"chirurgia\",\"chirurgiche\",\"cirugía\",\"cirurgia\",\"Chirurgie\",\"chirurgie\",\"heelkunde\",\"εγχείρηση\",\"επέμβαση\",\"χειρουργικήεπέμβαση\",\"cerrahi\",\"sebészet\",\"kirurgija\",\"kirurgia\",\"\"],\n" +
            "      \"key::81\": [\"cultural\",\"culturale\",\"culturali\",\"cultura\",\"cultural\",\"cultural\",\"kulturell\",\"cultureel\",\"πολιτιστικός\",\"πολιτιστική\",\"πολιτιστικό\",\"πολιτισμικός\",\"πολιτισμική\",\"πολιτισμικό\",\"kültürel\",\"kultúrális\",\"kulturni\",\"kultuuri\",\"kultuuriline\",\"\"],\n" +
            "      \"key::82\": [\"computerscience\",\"informatica\",\"ordenador\",\"computadora\",\"informática\",\"computación\",\"cienciasdelacomputación\",\"ciênciadacomputação\",\"Computer\",\"computer\",\"υπολογιστής\",\"ηλεκτρονικόςυπολογιστής\",\"bilgisayar\",\"számítógép\",\"računalnik\",\"arvuti\",\"\"],\n" +
            "      \"key::83\": [\"finance\",\"financial\",\"finanza\",\"finanziarie\",\"finanza\",\"financiero\",\"finanças\",\"financeiro\",\"Finanzen\",\"finanziell\",\"financiën\",\"financieel\",\"χρηματοοικονομικά\",\"χρηματοδότηση\",\"finanse\",\"finansal\",\"pénzügy\",\"pénzügyi\",\"finance\",\"finančni\",\"finants\",\"finantsiline\",\"\"],\n" +
            "      \"key::84\": [\"communication\",\"comunicazione\",\"comuniciación\",\"comunicação\",\"Kommunikation\",\"communication\",\"επικοινωνία\",\"iletişim\",\"kommunikáció\",\"komuniciranje\",\"kommunikatsioon\",\"\"],\n" +
            "      \"key::85\": [\"justice\",\"giustizia\",\"justicia\",\"justiça\",\"Recht\",\"Justiz\",\"justitie\",\"gerechtigheid\",\"δικαιοσύνη\",\"υπουργείοδικαιοσύνης\",\"δίκαιο\",\"adalet\",\"igazságügy\",\"pravo\",\"õigus\",\"\"],\n" +
            "      \"key::86\": [\"aerospace\",\"aerospaziale\",\"aerospaziali\",\"aeroespacio\",\"aeroespaço\",\"Luftfahrt\",\"luchtvaart\",\"ruimtevaart\",\"αεροπορικός\",\"αεροπορική\",\"αεροπορικό\",\"αεροναυπηγικός\",\"αεροναυπηγική\",\"αεροναυπηγικό\",\"αεροναυπηγικά\",\"havacılıkveuzay\",\"légtér\",\"zrakoplovstvo\",\"atmosfäär\",\"kosmos\",\"\"],\n" +
            "      \"key::87\": [\"dermatology\",\"dermatologia\",\"dermatología\",\"dermatologia\",\"Dermatologie\",\"dermatologie\",\"δρματολογία\",\"dermatoloji\",\"bőrgyógyászat\",\"dermatológia\",\"dermatologija\",\"dermatoloogia\",\"\"],\n" +
            "      \"key::88\": [\"architecture\",\"architettura\",\"arquitectura\",\"arquitetura\",\"Architektur\",\"architectuur\",\"αρχιτεκτονική\",\"mimarlık\",\"építészet\",\"arhitektura\",\"arhitektuur\",\"\"],\n" +
            "      \"key::89\": [\"mathematics\",\"matematica\",\"matematiche\",\"matemáticas\",\"matemáticas\",\"Mathematik\",\"wiskunde\",\"mathematica\",\"μαθηματικά\",\"matematik\",\"matematika\",\"matematika\",\"matemaatika\",\"\"],\n" +
            "      \"key::90\": [\"language\",\"lingue\",\"linguistica\",\"linguistiche\",\"lenguaje\",\"idioma\",\"língua\",\"idioma\",\"Sprache\",\"taal\",\"taalkunde\",\"γλώσσα\",\"dil\",\"nyelv\",\"jezik\",\"keel\",\"\"],\n" +
            "      \"key::91\": [\"neuroscience\",\"neuroscienza\",\"neurociencia\",\"neurociência\",\"Neurowissenschaft\",\"neurowetenschappen\",\"νευροεπιστήμη\",\"nörobilim\",\"idegtudomány\",\"nevroznanost\",\"neuroteadused\",\"\"],\n" +
            "      \"key::92\": [\"automation\",\"automazione\",\"automatización\",\"automação\",\"Automatisierung\",\"automatisering\",\"αυτοματοποίηση\",\"otomasyon\",\"automatizálás\",\"avtomatizacija\",\"automatiseeritud\",\"\"],\n" +
            "      \"key::93\": [\"pediatric\",\"pediatria\",\"pediatriche\",\"pediatrico\",\"pediátrico\",\"pediatría\",\"pediátrico\",\"pediatria\",\"pädiatrisch\",\"pediatrische\",\"παιδιατρική\",\"pediatrik\",\"gyermekgyógyászat\",\"pediatrija\",\"pediaatria\",\"\"],\n" +
            "      \"key::94\": [\"photonics\",\"fotonica\",\"fotoniche\",\"fotónica\",\"fotônica\",\"Photonik\",\"fotonica\",\"φωτονική\",\"fotonik\",\"fotonika\",\"fotonika\",\"fotoonika\",\"\"],\n" +
            "      \"key::95\": [\"mechanics\", \"mechanical\", \"meccanica\",\"meccaniche\",\"mecánica\",\"mecânica\",\"Mechanik\",\"Maschinenbau\",\"mechanica\",\"werktuigkunde\",\"μηχανικής\",\"mekanik\",\"gépészet\",\"mehanika\",\"mehaanika\",\"\"],\n" +
            "      \"key::96\": [\"psychiatrics\",\"psichiatria\",\"psichiatrica\",\"psichiatriche\",\"psiquiatría\",\"psiquiatria\",\"Psychiatrie\",\"psychiatrie\",\"ψυχιατρική\",\"psikiyatrik\",\"pszihiátria\",\"psihiatrija\",\"psühhaatria\",\"\"],\n" +
            "      \"key::97\": [\"psychology\",\"fisiologia\",\"psicología\",\"psicologia\",\"Psychologie\",\"psychologie\",\"ψυχολογία\",\"psikoloji\",\"pszihológia\",\"psihologija\",\"psühholoogia\",\"\"],\n" +
            "      \"key::98\": [\"automotive\",\"industriaautomobilistica\",\"industriadelautomóvil\",\"automotriz\",\"industriaautomotriz\",\"automotivo\",\"Automobilindustrie\",\"autoindustrie\",\"αυτοκίνητος\",\"αυτοκίνητη\",\"αυτοκίνητο\",\"αυτοκινούμενος\",\"αυτοκινούμενη\",\"αυτοκινούμενο\",\"αυτοκινητιστικός\",\"αυτοκινητιστική\",\"αυτοκινητιστικό\",\"otomotiv\",\"autóipari\",\"samogiben\",\"avtomobilskaindustrija\",\"auto-\",\"\"],\n" +
            "      \"key::99\": [\"neurology\",\"neurologia\",\"neurologiche\",\"neurología\",\"neurologia\",\"Neurologie\",\"neurologie\",\"zenuwleer\",\"νευρολογία\",\"nöroloji\",\"neurológia\",\"ideggyógyászat\",\"nevrologija\",\"neuroloogia\",\"\"],\n" +
            "      \"key::100\": [\"geology\",\"geologia\",\"geologiche\",\"geología\",\"geologia\",\"Geologie\",\"geologie\",\"aardkunde\",\"γεωλογία\",\"jeoloji\",\"geológia\",\"földtudomány\",\"geologija\",\"geoloogia\",\"\"],\n" +
            "      \"key::101\": [\"microbiology\",\"microbiologia\",\"micro-biologia\",\"microbiologiche\",\"microbiología\",\"microbiologia\",\"Mikrobiologie\",\"microbiologie\",\"μικροβιολογία\",\"mikrobiyoloji\",\"mikrobiológia\",\"mikrobiologija\",\"mikrobioloogia\",\"\"],\n" +
            "      \"key::102\": [\"informatics\",\"informatica\",\"informática\",\"informática\",\"informatica\",\"\"],\n" +
            "      \"key::103\": [\"forschungsgemeinschaft\",\"comunita ricerca\",\"research community\",\"research foundation\",\"research association\"],\n" +
            "      \"key::104\": [\"commerce\",\"ticaret\",\"ticarət\",\"commercio\",\"trade\",\"handel\",\"comercio\"],\n" +
            "      \"key::105\" : [\"state\", \"stato\", \"etade\", \"estado\", \"statale\", \"etat\", \"zustand\", \"estado\"],\n" +
            "      \"key::106\" : [\"seminary\", \"seminario\", \"seminaire\", \"seminar\"],\n" +
            "      \"key::107\" : [\"agricultural forestry\", \"af\", \"a f\"],\n" +
            "      \"key::108\" : [\"agricultural mechanical\", \"am\", \"a m\"],\n" +
            "      \"key::109\" : [\"catholic\", \"catholique\", \"katholische\", \"catolica\", \"cattolica\", \"catolico\"]\n" +
            "    }\n" +
            "  }\n" +
            "}");

    @Test
    public void testJPath () throws  Exception {

        MapDocument d = MapDocumentUtil.asMapDocumentWithJPath(conf, json);

        System.out.println("d = " + d);

    }
}
