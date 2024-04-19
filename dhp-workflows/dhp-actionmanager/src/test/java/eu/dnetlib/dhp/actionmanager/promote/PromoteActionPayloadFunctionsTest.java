
package eu.dnetlib.dhp.actionmanager.promote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.schema.oaf.Oaf;

public class PromoteActionPayloadFunctionsTest {

	private static SparkSession spark;

	@BeforeAll
	public static void beforeAll() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName(PromoteActionPayloadFunctionsTest.class.getSimpleName());
		conf.set("spark.driver.host", "localhost");
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@Nested
	class JoinTableWithActionPayloadAndMerge {

		@Test
		void shouldThrowWhenTableTypeIsNotSubtypeOfActionPayloadType() {
			// given
			class OafImpl extends Oaf {
			}

			// when
			assertThrows(
				RuntimeException.class,
				() -> PromoteActionPayloadFunctions
					.joinGraphTableWithActionPayloadAndMerge(
						null, null, null, null, null, null, OafImplSubSub.class, OafImpl.class));
		}

		@Test
		void shouldRunProperlyWhenActionPayloadTypeAndTableTypeAreTheSame() {
			// given
			String id0 = "id0";
			String id1 = "id1";
			String id2 = "id2";
			String id3 = "id3";
			String id4 = "id4";
			List<OafImplSubSub> rowData = Arrays
				.asList(
					createOafImplSubSub(id0),
					createOafImplSubSub(id1),
					createOafImplSubSub(id2),
					createOafImplSubSub(id3));
			Dataset<OafImplSubSub> rowDS = spark.createDataset(rowData, Encoders.bean(OafImplSubSub.class));

			List<OafImplSubSub> actionPayloadData = Arrays
				.asList(
					createOafImplSubSub(id1),
					createOafImplSubSub(id2),
					createOafImplSubSub(id2),
					createOafImplSubSub(id3),
					createOafImplSubSub(id3),
					createOafImplSubSub(id3),
					createOafImplSubSub(id4),
					createOafImplSubSub(id4),
					createOafImplSubSub(id4),
					createOafImplSubSub(id4));
			Dataset<OafImplSubSub> actionPayloadDS = spark
				.createDataset(actionPayloadData, Encoders.bean(OafImplSubSub.class));

			SerializableSupplier<Function<OafImplSubSub, String>> rowIdFn = () -> OafImplRoot::getId;
			SerializableSupplier<Function<OafImplSubSub, String>> actionPayloadIdFn = () -> OafImplRoot::getId;
			SerializableSupplier<BiFunction<OafImplSubSub, OafImplSubSub, OafImplSubSub>> mergeAndGetFn = () -> (x,
				y) -> {
				x.merge(y);
				return x;
			};

			// when
			List<OafImplSubSub> results = PromoteActionPayloadFunctions
				.joinGraphTableWithActionPayloadAndMerge(
					rowDS,
					actionPayloadDS,
					rowIdFn,
					actionPayloadIdFn,
					mergeAndGetFn,
					PromoteAction.Strategy.UPSERT,
					OafImplSubSub.class,
					OafImplSubSub.class)
				.collectAsList();

			// then
			assertEquals(11, results.size());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id0)).count());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id1)).count());
			assertEquals(2, results.stream().filter(x -> x.getId().equals(id2)).count());
			assertEquals(3, results.stream().filter(x -> x.getId().equals(id3)).count());
			assertEquals(4, results.stream().filter(x -> x.getId().equals(id4)).count());

			results
				.forEach(
					result -> {
						switch (result.getId()) {
							case "id0":
								assertEquals(1, result.getMerged());
								break;
							case "id1":
							case "id2":
							case "id3":
								assertEquals(2, result.getMerged());
								break;
							case "id4":
								assertEquals(1, result.getMerged());
								break;
							default:
								throw new RuntimeException();
						}
					});
		}

		@Test
		void shouldRunProperlyWhenActionPayloadTypeIsSuperTypeOfTableType() {
			// given
			String id0 = "id0";
			String id1 = "id1";
			String id2 = "id2";
			String id3 = "id3";
			String id4 = "id4";
			List<OafImplSubSub> rowData = Arrays
				.asList(
					createOafImplSubSub(id0),
					createOafImplSubSub(id1),
					createOafImplSubSub(id2),
					createOafImplSubSub(id3));
			Dataset<OafImplSubSub> rowDS = spark.createDataset(rowData, Encoders.bean(OafImplSubSub.class));

			List<OafImplSub> actionPayloadData = Arrays
				.asList(
					createOafImplSub(id1),
					createOafImplSub(id2),
					createOafImplSub(id2),
					createOafImplSub(id3),
					createOafImplSub(id3),
					createOafImplSub(id3),
					createOafImplSub(id4),
					createOafImplSub(id4),
					createOafImplSub(id4),
					createOafImplSub(id4));
			Dataset<OafImplSub> actionPayloadDS = spark
				.createDataset(actionPayloadData, Encoders.bean(OafImplSub.class));

			SerializableSupplier<Function<OafImplSubSub, String>> rowIdFn = () -> OafImplRoot::getId;
			SerializableSupplier<Function<OafImplSub, String>> actionPayloadIdFn = () -> OafImplRoot::getId;
			SerializableSupplier<BiFunction<OafImplSubSub, OafImplSub, OafImplSubSub>> mergeAndGetFn = () -> (x, y) -> {
				x.merge(y);
				return x;
			};

			// when
			List<OafImplSubSub> results = PromoteActionPayloadFunctions
				.joinGraphTableWithActionPayloadAndMerge(
					rowDS,
					actionPayloadDS,
					rowIdFn,
					actionPayloadIdFn,
					mergeAndGetFn,
					PromoteAction.Strategy.UPSERT,
					OafImplSubSub.class,
					OafImplSub.class)
				.collectAsList();

			// then
			assertEquals(7, results.size());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id0)).count());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id1)).count());
			assertEquals(2, results.stream().filter(x -> x.getId().equals(id2)).count());
			assertEquals(3, results.stream().filter(x -> x.getId().equals(id3)).count());
			assertEquals(0, results.stream().filter(x -> x.getId().equals(id4)).count());

			results
				.forEach(
					result -> {
						switch (result.getId()) {
							case "id0":
								assertEquals(1, result.getMerged());
								break;
							case "id1":
							case "id2":
							case "id3":
								assertEquals(2, result.getMerged());
								break;
							default:
								throw new RuntimeException();
						}
					});
		}
	}

	@Nested
	class GroupTableByIdAndMerge {

		@Test
		void shouldRunProperly() {
			// given
			String id1 = "id1";
			String id2 = "id2";
			String id3 = "id3";
			List<OafImplSubSub> rowData = Arrays
				.asList(
					createOafImplSubSub(id1),
					createOafImplSubSub(id2),
					createOafImplSubSub(id2),
					createOafImplSubSub(id3),
					createOafImplSubSub(id3),
					createOafImplSubSub(id3));
			Dataset<OafImplSubSub> rowDS = spark.createDataset(rowData, Encoders.bean(OafImplSubSub.class));

			SerializableSupplier<Function<OafImplSubSub, String>> rowIdFn = () -> OafImplRoot::getId;
			SerializableSupplier<BiFunction<OafImplSubSub, OafImplSubSub, OafImplSubSub>> mergeAndGetFn = () -> (x,
				y) -> {
				x.merge(y);
				return x;
			};
			SerializableSupplier<OafImplSubSub> zeroFn = OafImplSubSub::new;
			SerializableSupplier<Function<OafImplSubSub, Boolean>> isNotZeroFn = () -> x -> Objects.nonNull(x.getId());

			// when
			List<OafImplSubSub> results = PromoteActionPayloadFunctions
				.groupGraphTableByIdAndMerge(
					rowDS, rowIdFn, mergeAndGetFn, zeroFn, isNotZeroFn, OafImplSubSub.class)
				.collectAsList();

			// then
			assertEquals(3, results.size());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id1)).count());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id2)).count());
			assertEquals(1, results.stream().filter(x -> x.getId().equals(id3)).count());

			results
				.forEach(
					result -> {
						switch (result.getId()) {
							case "id1":
								assertEquals(1, result.getMerged());
								break;
							case "id2":
								assertEquals(2, result.getMerged());
								break;
							case "id3":
								assertEquals(3, result.getMerged());
								break;
							default:
								throw new RuntimeException();
						}
					});
		}
	}

	public static class OafImplRoot extends Oaf {
		private String id;
		private int merged = 1;

		public void merge(OafImplRoot e) {
			merged += e.merged;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public int getMerged() {
			return merged;
		}

		public void setMerged(int merged) {
			this.merged = merged;
		}
	}

	public static class OafImplSub extends OafImplRoot {

		@Override
		public void merge(OafImplRoot e) {
			super.merge(e);
		}
	}

	private static OafImplSub createOafImplSub(String id) {
		OafImplSub x = new OafImplSub();
		x.setId(id);
		return x;
	}

	public static class OafImplSubSub extends OafImplSub {

		@Override
		public void merge(OafImplRoot e) {
			super.merge(e);
		}
	}

	private static OafImplSubSub createOafImplSubSub(String id) {
		OafImplSubSub x = new OafImplSubSub();
		x.setId(id);
		return x;
	}
}
