package eu.dnetlib.dhp.bulktag;

import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import eu.dnetlib.dhp.bulktag.actions.MapModel;
import eu.dnetlib.dhp.bulktag.actions.Parameters;
import eu.dnetlib.dhp.schema.oaf.Result;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class Utils implements Serializable {
    public static Map<String, List<String>> getParamMap(final Result result, Map<String, MapModel> params)
            throws NoSuchMethodException, InvocationTargetException {
        Map<String, List<String>> param = new HashMap<>();
        String json = new Gson().toJson(result, Result.class);
        DocumentContext jsonContext = JsonPath.parse(json);

        if (params == null) {
            params = new HashMap<>();
        }
        for (String key : params.keySet()) {
            MapModel mapModel = params.get(key);

            try {
                String path = mapModel.getPath();
                Object obj = jsonContext.read(path);
                List<String> pathValue;
                if (obj instanceof java.lang.String)
                    pathValue = Arrays.asList((String) obj);
                else
                    pathValue = (List<String>) obj;
                if (Optional.ofNullable(mapModel.getAction()).isPresent()) {
                    Class<?> c = Class.forName(mapModel.getAction().getClazz());
                    Object class_instance = c.newInstance();
                    Method setField = c.getMethod("setValue", String.class);
                    setField.invoke(class_instance, pathValue.get(0));
                    for (Parameters p : mapModel.getAction().getParams()) {
                        setField = c.getMethod("set" + p.getParamName(), String.class);
                        setField.invoke(class_instance, p.getParamValue());
                    }

                    param
                            .put(
                                    key, Arrays
                                            .asList((String) c.getMethod(mapModel.getAction().getMethod()).invoke(class_instance)));

                }

                else {
                    param.put(key, pathValue);
                }

            } catch (PathNotFoundException | ClassNotFoundException | InstantiationException
                     | IllegalAccessException e) {
                param.put(key, new ArrayList<>());
            }
        }
        return param;

    }
}
