
package eu.dnetlib.dhp.oa.dedup;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class DedupRecordFactory {

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private DedupRecordFactory() {
	}

	public static <T extends OafEntity> Dataset<T> createDedupRecord(
		final SparkSession spark,
		final DataInfo dataInfo,
		final String mergeRelsInputPath,
		final String entitiesInputPath,
		final Class<T> clazz) {

		long ts = System.currentTimeMillis();

		// <id, json_entity>
		Dataset<Tuple2<String, T>> entities = spark
			.read()
			.textFile(entitiesInputPath)
			.map(
				(MapFunction<String, Tuple2<String, T>>) it -> {
					T entity = OBJECT_MAPPER.readValue(it, clazz);
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

		// <source, target>: source is the dedup_id, target is the id of the mergedIn
		Dataset<Tuple2<String, String>> mergeRels = spark
			.read()
			.load(mergeRelsInputPath)
			.as(Encoders.bean(Relation.class))
			.where("relClass == 'merges'")
			.map(
				(MapFunction<Relation, Tuple2<String, String>>) r -> new Tuple2<>(r.getSource(), r.getTarget()),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		return mergeRels
			.joinWith(entities, mergeRels.col("_2").equalTo(entities.col("_1")), "inner")
			.map(
				(MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, T>>, Tuple2<String, T>>) value -> new Tuple2<>(
					value._1()._1(), value._2()._2()),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)))
			.groupByKey(
				(MapFunction<Tuple2<String, T>, String>) Tuple2::_1, Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<String, T>, T>) (key,
					values) -> entityMerger(key, values, ts, dataInfo, clazz),
				Encoders.bean(clazz));
	}

	public static <T extends OafEntity> T entityMerger(
		String id, Iterator<Tuple2<String, T>> entities, long ts, DataInfo dataInfo, Class<T> clazz)
		throws IllegalAccessException, InstantiationException, InvocationTargetException {

		final Comparator<Identifier<T>> idComparator = new IdentifierComparator<>();

		final LinkedList<T> entityList = Lists
			.newArrayList(entities)
			.stream()
			.map(t -> Identifier.newInstance(t._2()))
			.sorted(idComparator)
			.map(Identifier::getEntity)
			.collect(Collectors.toCollection(LinkedList::new));

		final T entity = clazz.newInstance();
		final T first = entityList.removeFirst();

		BeanUtils.copyProperties(entity, first);

		final List<List<Author>> authors = Lists.newArrayList();

		entityList
			.forEach(
				duplicate -> {
					entity.mergeFrom(duplicate);
					if (ModelSupport.isSubClass(duplicate, Result.class)) {
						Result r1 = (Result) duplicate;
						Optional
							.ofNullable(r1.getAuthor())
							.ifPresent(a -> authors.add(a));
					}
				});

		// set authors and date
		if (ModelSupport.isSubClass(entity, Result.class)) {
			Optional
				.ofNullable(((Result) entity).getAuthor())
				.ifPresent(a -> authors.add(a));

			((Result) entity).setAuthor(AuthorMerger.merge(authors));
		}

		entity.setId(id);

		entity.setLastupdatetimestamp(ts);
		entity.setDataInfo(dataInfo);

		return entity;
	}

}
