
package eu.dnetlib.pace.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.Cache;
import com.jayway.jsonpath.spi.cache.CacheProvider;

import eu.dnetlib.pace.config.Type;
import net.minidev.json.JSONArray;

public class MapDocumentUtil {

	public static final String URL_REGEX = "^(http|https|ftp)\\://.*";
	public static Predicate<String> urlFilter = s -> s.trim().matches(URL_REGEX);

	static {
		CacheProvider.setCache(new Cache() {
			private final ConcurrentHashMap<String, JsonPath> jsonPathCache = new ConcurrentHashMap();

			@Override
			public JsonPath get(String key) {
				return jsonPathCache.get(key);
			}

			@Override
			public void put(String key, JsonPath value) {
				jsonPathCache.put(key, value);
			}
		});
	}

	public static String getJPathString(final String jsonPath, final String json) {
		try {
			Object o = JsonPath.read(json, jsonPath);
			if (o instanceof String)
				return (String) o;
			if (o instanceof JSONArray && ((JSONArray) o).size() > 0)
				return (String) ((JSONArray) o).get(0);
			return "";
		} catch (Exception e) {
			return "";
		}
	}

	public static double[] getJPathArray(final String jsonPath, final String json) {
		try {
			Object o = JsonPath.read(json, jsonPath);
			if (o instanceof double[])
				return (double[]) o;
			if (o instanceof JSONArray) {
				Object[] objects = ((JSONArray) o).toArray();
				double[] array = new double[objects.length];
				for (int i = 0; i < objects.length; i++) {
					if (objects[i] instanceof BigDecimal)
						array[i] = ((BigDecimal) objects[i]).doubleValue();
					else
						array[i] = (double) objects[i];
				}
				return array;
			}
			return new double[0];
		} catch (Exception e) {
			e.printStackTrace();
			return new double[0];
		}
	}

	public static String truncateValue(String value, int length) {
		if (value == null)
			return "";

		if (length == -1 || length > value.length())
			return value;

		return value.substring(0, length);
	}

	public static List<String> truncateList(List<String> list, int size) {
		if (size == -1 || size > list.size())
			return list;

		return list.subList(0, size);
	}

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
		// if (type == Type.List)
		// return JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST,
		// Option.SUPPRESS_EXCEPTIONS)).parse(json).read(path);
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
			});
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
