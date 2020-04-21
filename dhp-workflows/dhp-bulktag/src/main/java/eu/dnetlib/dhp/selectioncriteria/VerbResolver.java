package eu.dnetlib.dhp.selectioncriteria;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Collectors;
import org.reflections.Reflections;

public class VerbResolver implements Serializable {
    private final Map<String, Class<Selection>> map;

    public VerbResolver() {

        this.map =
                new Reflections("eu.dnetlib")
                        .getTypesAnnotatedWith(VerbClass.class).stream()
                                .collect(
                                        Collectors.toMap(
                                                v -> v.getAnnotation(VerbClass.class).value(),
                                                v -> (Class<Selection>) v));
    }

    public Selection getSelectionCriteria(String name, String param)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                    InstantiationException {

        return map.get(name).getDeclaredConstructor((String.class)).newInstance(param);
    }
}
