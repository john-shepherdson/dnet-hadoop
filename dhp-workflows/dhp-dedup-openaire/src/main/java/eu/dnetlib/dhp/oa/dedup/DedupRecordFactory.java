
package eu.dnetlib.dhp.oa.dedup;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
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
			.groupByKey((MapFunction<Tuple3<String, String, OafEntity>, String>) Tuple3::_1, Encoders.STRING())
			.flatMapGroups(
				(FlatMapGroupsFunction<String, Tuple3<String, String, OafEntity>, OafEntity>) (dedupId, it) -> {
					if (!it.hasNext())
						return Collections.emptyIterator();

					final ArrayList<OafEntity> cliques = new ArrayList<>();

					final ArrayList<String> aliases = new ArrayList<>();

					final HashSet<String> acceptanceDate = new HashSet<>();

					boolean isVisible = false;

					while (it.hasNext()) {
						Tuple3<String, String, OafEntity> t = it.next();
						OafEntity entity = t._3();

						if (entity == null) {
							aliases.add(t._2());
						} else {
							isVisible = isVisible || !entity.getDataInfo().getInvisible();
							cliques.add(entity);

							if (acceptanceDate.size() < MAX_ACCEPTANCE_DATE) {
								if (Result.class.isAssignableFrom(entity.getClass())) {
									Result result = (Result) entity;
									if (result.getDateofacceptance() != null
										&& StringUtils.isNotBlank(result.getDateofacceptance().getValue())) {
										acceptanceDate.add(result.getDateofacceptance().getValue());
									}
								}
							}
						}

					}

					if (!isVisible || acceptanceDate.size() >= MAX_ACCEPTANCE_DATE || cliques.isEmpty()) {
						return Collections.emptyIterator();
					}

					OafEntity mergedEntity = MergeUtils.mergeGroup(dedupId, cliques.iterator());
					// dedup records do not have date of transformation attribute
					mergedEntity.setDateoftransformation(null);
					mergedEntity
						.setMergedIds(
							Stream
								.concat(cliques.stream().map(OafEntity::getId), aliases.stream())
								.distinct()
								.sorted()
								.collect(Collectors.toList()));

					return Stream
						.concat(
							Stream
								.of(dedupId)
								.map(id -> createDedupOafEntity(id, mergedEntity, dataInfo, ts)),
							aliases
								.stream()
								.map(id -> createMergedDedupAliasOafEntity(id, mergedEntity, dataInfo, ts)))
						.iterator();

				}, beanEncoder);
	}

	private static OafEntity createDedupOafEntity(String id, OafEntity base, DataInfo dataInfo, long ts) {
		try {
			OafEntity res = (OafEntity) BeanUtils.cloneBean(base);
			res.setId(id);
			res.setDataInfo(dataInfo);
			res.setLastupdatetimestamp(ts);
			return res;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static OafEntity createMergedDedupAliasOafEntity(String id, OafEntity base, DataInfo dataInfo, long ts) {
		try {
			OafEntity res = createDedupOafEntity(id, base, dataInfo, ts);
			DataInfo ds = (DataInfo) BeanUtils.cloneBean(dataInfo);
			ds.setDeletedbyinference(true);
			res.setDataInfo(ds);
			return res;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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

		entity = MergeUtils.checkedMerge(entity, duplicate, false);

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
