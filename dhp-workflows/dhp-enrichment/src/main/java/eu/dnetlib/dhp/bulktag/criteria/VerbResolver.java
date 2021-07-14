
package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Collectors;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

public class VerbResolver implements Serializable {
	private Map<String, Class<Selection>> map = null; // = new HashMap<>();
	private final ClassGraph classgraph = new ClassGraph();

	public VerbResolver() {

		try (ScanResult scanResult = // Assign scanResult in try-with-resources
			classgraph // Create a new ClassGraph instance
				.verbose() // If you want to enable logging to stderr
				.enableAllInfo() // Scan classes, methods, fields, annotations
				.whitelistPackages(
					"eu.dnetlib.dhp.bulktag.criteria") // Scan com.xyz and subpackages
				.scan()) { // Perform the scan and return a ScanResult

			ClassInfoList routeClassInfoList = scanResult
				.getClassesWithAnnotation(
					"eu.dnetlib.dhp.bulktag.criteria.VerbClass");

			this.map = routeClassInfoList
				.stream()
				.collect(
					Collectors
						.toMap(
							value -> (String) value
								.getAnnotationInfo()
								.get(0)
								.getParameterValues()
								.get(0)
								.getValue(),
							value -> (Class<Selection>) value.loadClass()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Selection getSelectionCriteria(String name, String param)
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
		InstantiationException {

		// return Class.forName(tmp_map.get(name)).
		return map.get(name).getDeclaredConstructor((String.class)).newInstance(param);
	}
}
