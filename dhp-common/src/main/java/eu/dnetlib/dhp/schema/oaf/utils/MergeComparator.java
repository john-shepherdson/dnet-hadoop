
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Collectors;

// 
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;

public class MergeComparator implements Comparator<Oaf> {
	public MergeComparator() {
	}

	public int compare(Oaf left, Oaf right) {
		// nulls at the end
		if (left == null && right == null) {
			return 0;
		} else if (left == null) {
			return -1;
		} else if (right == null) {
			return 1;
		}

		// invisible
		if (left.getDataInfo() != null && left.getDataInfo().getInvisible() == true) {
			if (right.getDataInfo() != null && right.getDataInfo().getInvisible() == false) {
				return -1;
			}
		}

		// collectedfrom
		HashSet<String> lCf = getCollectedFromIds(left);
		HashSet<String> rCf = getCollectedFromIds(right);
		if (lCf.contains("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")
			&& !rCf.contains("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")) {
			return -1;
		} else if (!lCf.contains("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")
			&& rCf.contains("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")) {
			return 1;
		}

		SubEntityType lClass = SubEntityType.fromClass(left.getClass());
		SubEntityType rClass = SubEntityType.fromClass(right.getClass());
		return lClass.ordinal() - rClass.ordinal();

	}

	protected HashSet<String> getCollectedFromIds(Oaf left) {
		return (HashSet) Optional.ofNullable(left.getCollectedfrom()).map((cf) -> {
			return (HashSet) cf.stream().map(KeyValue::getKey).collect(Collectors.toCollection(HashSet::new));
		}).orElse(new HashSet());
	}

	enum SubEntityType {
		publication, dataset, software, otherresearchproduct, datasource, organization, project;

		/**
		 * Resolves the EntityType, given the relative class name
		 *
		 * @param clazz the given class name
		 * @param <T> actual OafEntity subclass
		 * @return the EntityType associated to the given class
		 */
		public static <T extends Oaf> SubEntityType fromClass(Class<T> clazz) {
			return valueOf(clazz.getSimpleName().toLowerCase());
		}
	}

}
