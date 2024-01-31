
package eu.dnetlib.dhp.oa.dedup;

import java.util.*;
import java.util.stream.Stream;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;

public class DedupRecordFactory {
	public static final class DedupRecordReduceState {
		public final String dedupId;

		public final ArrayList<String> aliases = new ArrayList<>();

		public final HashSet<String> acceptanceDate = new HashSet<>();

		public OafEntity entity;

		public DedupRecordReduceState(String dedupId, String id, OafEntity entity) {
			this.dedupId = dedupId;
			this.entity = entity;
			if (entity == null) {
				aliases.add(id);
			} else {
				if (Result.class.isAssignableFrom(entity.getClass())) {
					Result result = (Result) entity;
					if (result.getDateofacceptance() != null
						&& StringUtils.isNotBlank(result.getDateofacceptance().getValue())) {
						acceptanceDate.add(result.getDateofacceptance().getValue());
					}
				}
			}
		}

		public String getDedupId() {
			return dedupId;
		}
	}

	private static final int MAX_ACCEPTANCE_DATE = 20;

	private DedupRecordFactory() {
	}

	public static Dataset<OafEntity> createDedupRecord(
		final SparkSession spark,
		final DataInfo dataInfo,
		final String mergeRelsInputPath,
		final String entitiesInputPath,
		final Class<OafEntity> clazz) {

		final long ts = System.currentTimeMillis();
		final Encoder<OafEntity> beanEncoder = Encoders.bean(clazz);
		final Encoder<OafEntity> kryoEncoder = Encoders.kryo(clazz);

		// <id, json_entity>
		Dataset<Row> entities = spark
			.read()
			.schema(Encoders.bean(clazz).schema())
			.json(entitiesInputPath)
			.as(beanEncoder)
			.map(
				(MapFunction<OafEntity, Tuple2<String, OafEntity>>) entity -> {
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), kryoEncoder))
			.selectExpr("_1 AS id", "_2 AS kryoObject");

		// <source, target>: source is the dedup_id, target is the id of the mergedIn
		Dataset<Row> mergeRels = spark
			.read()
			.load(mergeRelsInputPath)
			.where("relClass == 'merges'")
			.selectExpr("source as dedupId", "target as id");

		return mergeRels
			.join(entities, JavaConversions.asScalaBuffer(Collections.singletonList("id")), "left")
			.select("dedupId", "id", "kryoObject")
			.as(Encoders.tuple(Encoders.STRING(), Encoders.STRING(), kryoEncoder))
			.map(
				(MapFunction<Tuple3<String, String, OafEntity>, DedupRecordReduceState>) t -> new DedupRecordReduceState(
					t._1(), t._2(), t._3()),
				Encoders.kryo(DedupRecordReduceState.class))
			.groupByKey(
				(MapFunction<DedupRecordReduceState, String>) DedupRecordReduceState::getDedupId, Encoders.STRING())
			.reduceGroups(
				(ReduceFunction<DedupRecordReduceState>) (t1, t2) -> {
					if (t1.entity == null) {
						t2.aliases.addAll(t1.aliases);
						return t2;
					}
					if (t1.acceptanceDate.size() < MAX_ACCEPTANCE_DATE) {
						t1.acceptanceDate.addAll(t2.acceptanceDate);
					}
					t1.aliases.addAll(t2.aliases);
					t1.entity = reduceEntity(t1.entity, t2.entity);

					return t1;
				})
			.flatMap((FlatMapFunction<Tuple2<String, DedupRecordReduceState>, OafEntity>) t -> {
				String dedupId = t._1();
				DedupRecordReduceState agg = t._2();

				if (agg.acceptanceDate.size() >= MAX_ACCEPTANCE_DATE) {
					return Collections.emptyIterator();
				}

				return Stream
					.concat(Stream.of(agg.getDedupId()), agg.aliases.stream())
					.map(id -> {
						try {
							OafEntity res = (OafEntity) BeanUtils.cloneBean(agg.entity);
							res.setId(id);
							res.setDataInfo(dataInfo);
							res.setLastupdatetimestamp(ts);
							return res;
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					})
					.iterator();
			}, beanEncoder);
	}

	private static OafEntity reduceEntity(OafEntity entity, OafEntity duplicate) {

		if (duplicate == null) {
			return entity;
		}

		int compare = new IdentifierComparator<>()
			.compare(Identifier.newInstance(entity), Identifier.newInstance(duplicate));

		if (compare > 0) {
			OafEntity swap = duplicate;
			duplicate = entity;
			entity = swap;
		}

		entity.mergeFrom(duplicate);

		if (ModelSupport.isSubClass(duplicate, Result.class)) {
			Result re = (Result) entity;
			Result rd = (Result) duplicate;

			List<List<Author>> authors = new ArrayList<>();
			if (re.getAuthor() != null) {
				authors.add(re.getAuthor());
			}
			if (rd.getAuthor() != null) {
				authors.add(rd.getAuthor());
			}

			re.setAuthor(AuthorMerger.merge(authors));
		}

		return entity;
	}

	public static <T extends OafEntity> T entityMerger(
		String id, Iterator<Tuple2<String, T>> entities, long ts, DataInfo dataInfo, Class<T> clazz) {
		T base = entities.next()._2();

		while (entities.hasNext()) {
			T duplicate = entities.next()._2();
			if (duplicate != null)
				base = (T) reduceEntity(base, duplicate);
		}

		base.setId(id);
		base.setDataInfo(dataInfo);
		base.setLastupdatetimestamp(ts);

		return base;
	}

}
