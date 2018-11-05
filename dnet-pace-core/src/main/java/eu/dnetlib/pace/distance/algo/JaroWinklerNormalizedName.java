package eu.dnetlib.pace.distance.algo;

import com.wcohen.ss.AbstractStringDistance;
import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.distance.DistanceClass;
import eu.dnetlib.pace.distance.SecondStringDistanceAlgo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@DistanceClass("JaroWinklerNormalizedName")
public class JaroWinklerNormalizedName extends SecondStringDistanceAlgo {

    private static Set<String> stopwordsEn = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_en.txt");
    private static Set<String> stopwordsIt = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_it.txt");
    private static Set<String> stopwordsDe = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_de.txt");
    private static Set<String> stopwordsFr = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_fr.txt");
    private static Set<String> stopwordsPt = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_pt.txt");
    private static Set<String> stopwordsEs = AbstractPaceFunctions.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_es.txt");

    //key=word, value=global identifier => example: "università"->"university", used to substitute the word with the global identifier
    private static Map<String,String> translationMap = AbstractPaceFunctions.loadMapFromClasspath("/eu/dnetlib/pace/config/translation_map.csv");

    public JaroWinklerNormalizedName(Map<String, Number> params){
        super(params, new com.wcohen.ss.JaroWinkler());
    }

    public JaroWinklerNormalizedName(double weight) {
        super(weight, new com.wcohen.ss.JaroWinkler());
    }

    protected JaroWinklerNormalizedName(double weight, AbstractStringDistance ssalgo) {
        super(weight, ssalgo);
    }

    @Override
    public double distance(String a, String b) {
        String ca = cleanup(a);
        String cb = cleanup(b);

        ca = removeStopwords(ca);
        cb = removeStopwords(cb);

        //replace keywords with codes
        ca = translate(ca, translationMap);
        cb = translate(cb, translationMap);

        if (sameKeywords(ca,cb)) {
            return normalize(ssalgo.score(removeCodes(ca), removeCodes(cb)));
        }
        return 0.0;
    }

    @Override
    public double getWeight() {
        return super.weight;
    }

    @Override
    protected double normalize(double d) {
        return d;
    }

    public String removeStopwords(String s) {
        String normString = normalize(s);

        normString = filterStopWords(normString, stopwordsIt);
        normString = filterStopWords(normString, stopwordsEn);
        normString = filterStopWords(normString, stopwordsDe);
        normString = filterStopWords(normString, stopwordsFr);
        normString = filterStopWords(normString, stopwordsPt);
        normString = filterStopWords(normString, stopwordsEs);

        return normString;
    }
}
