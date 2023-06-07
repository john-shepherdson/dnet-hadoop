package eu.dnetlib.dhp.oa.dedup.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import eu.dnetlib.dhp.oa.dedup.DedupUtility;
import eu.dnetlib.dhp.oa.dedup.SparkReporter;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.config.Type;
import eu.dnetlib.pace.model.*;
import eu.dnetlib.pace.util.BlockProcessor;
import eu.dnetlib.pace.util.MapDocumentUtil;
import net.minidev.json.JSONArray;
import org.apache.commons.compress.utils.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DFMapDocumentUtils extends MapDocumentUtil {
    public static final Pattern URL_REGEX = Pattern.compile("^\\s*(http|https|ftp)\\://.*");

    public static final Pattern CONCAT_REGEX = Pattern.compile("\\|\\|\\|");
    public static Predicate<String> urlFilter = s -> URL_REGEX.matcher(s).matches();

    public static String getJPathString(final String jsonPath, final DocumentContext json) {
        try {
            Object o = json.read(jsonPath);
            if (o instanceof String)
                return (String) o;
            if (o instanceof JSONArray && ((JSONArray) o).size() > 0)
                return (String) ((JSONArray) o).get(0);
            return "";
        } catch (Exception e) {
            return "";
        }
    }

    public static List<String> getJPathList(String path, DocumentContext json, Type type) {
        //      if (type == Type.List)
        //          return JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST, Option.SUPPRESS_EXCEPTIONS)).parse(json).read(path);
        Object jresult;
        List<String> result = new ArrayList<>();
        try {
            jresult = json.read(path);
        } catch (Throwable e) {
            return result;
        }

        if (jresult instanceof JSONArray) {
            ((JSONArray) jresult).forEach(it -> {
                        try {
                            result.add(new ObjectMapper().writeValueAsString(it));
                        } catch (JsonProcessingException e) {

                        }
                    }
            );
            return result;
        }

        if (jresult instanceof LinkedHashMap) {
            try {
                result.add(new ObjectMapper().writeValueAsString(jresult));
            } catch (JsonProcessingException e) {

            }
            return result;
        }
        if (jresult instanceof String) {
            result.add((String) jresult);
        }
        return result;
    }
}