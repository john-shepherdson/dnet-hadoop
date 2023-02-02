
package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.schema.oaf.common.ModelSupport.isSubClass;

import java.util.function.BiFunction;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.schema.oaf.*;

import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;

/** OAF model merging support. */
public class MergeAndGet {

	private MergeAndGet() {
	}

	/**
	 * Strategy for merging OAF model objects.
	 * <p>
	 * MERGE_FROM_AND_GET: use OAF 'mergeFrom' method SELECT_NEWER_AND_GET: use last update timestamp to return newer
	 * instance
	 */
	public enum Strategy {
		MERGE_FROM_AND_GET, SELECT_NEWER_AND_GET
	}

	/**
	 * Returns a function for merging OAF model objects.
	 *
	 * @param strategy Strategy to be used to merge objects
	 * @param <G> Graph table type
	 * @param <A> Action payload type
	 * @return BiFunction to be used to merge OAF objects
	 */
	public static <G extends Oaf, A extends Oaf> SerializableSupplier<BiFunction<G, A, G>> functionFor(
		Strategy strategy) {
		switch (strategy) {
			case MERGE_FROM_AND_GET:
				return () -> MergeAndGet::mergeFromAndGet;
			case SELECT_NEWER_AND_GET:
				return () -> MergeAndGet::selectNewerAndGet;
		}
		throw new RuntimeException();
	}

	private static <G extends Oaf, A extends Oaf> G mergeFromAndGet(G x, A y) {
		if (isSubClass(x, Relation.class) && isSubClass(y, Relation.class)) {
			return (G) MergeUtils.mergeRelation((Relation) x, (Relation) y);
		} else if (isSubClass(x, Result.class)
			&& isSubClass(y, Result.class)
			&& isSubClass(x, y)) {
			return (G) MergeUtils.mergeResult((Result) x, (Result) y);
		} else if (isSubClass(x, Datasource.class)
				&& isSubClass(y, Datasource.class)
				&& isSubClass(x, y)) {
			throw new RuntimeException("MERGE_FROM_AND_GET should not deal with Datasource types");
		} else if (isSubClass(x, Organization.class)
				&& isSubClass(y, Organization.class)
				&& isSubClass(x, y)) {
			return (G) MergeUtils.mergeOrganization((Organization) x, (Organization) y);
		} else if (isSubClass(x, Project.class)
				&& isSubClass(y, Project.class)
				&& isSubClass(x, y)) {
			return (G) MergeUtils.mergeProject((Project) x, (Project) y);
		}
		throw new RuntimeException(
			String
				.format(
					"MERGE_FROM_AND_GET incompatible types: %s, %s",
					x.getClass().getCanonicalName(), y.getClass().getCanonicalName()));
	}

	@SuppressWarnings("unchecked")
	private static <G extends Oaf, A extends Oaf> G selectNewerAndGet(G x, A y) {
		if (isSubClass(x, Entity.class) && isSubClass(x, Entity.class)) {
			Entity xE = (Entity) x;
			Entity yE = (Entity) y;

			if (xE.getClass().equals(yE.getClass())
					&& xE.getLastupdatetimestamp() > yE.getLastupdatetimestamp()) {
				return x;
			} else if (xE.getClass().equals(yE.getClass())
					&& xE.getLastupdatetimestamp() < yE.getLastupdatetimestamp()) {
				return (G) y;
			} else if (isSubClass(xE, yE) && xE.getLastupdatetimestamp() > yE.getLastupdatetimestamp()) {
				return x;
			} else if (isSubClass(xE, yE) && xE.getLastupdatetimestamp() < yE.getLastupdatetimestamp()) {
				throw new RuntimeException(
						String
								.format(
										"SELECT_NEWER_AND_GET cannot return right type when it is not the same as left type: %s, %s",
										x.getClass().getCanonicalName(), y.getClass().getCanonicalName()));
			}

		}
		throw new RuntimeException(
			String
				.format(
					"SELECT_NEWER_AND_GET cannot be used when left is not subtype of right: %s, %s",
					x.getClass().getCanonicalName(), y.getClass().getCanonicalName()));
	}
}
