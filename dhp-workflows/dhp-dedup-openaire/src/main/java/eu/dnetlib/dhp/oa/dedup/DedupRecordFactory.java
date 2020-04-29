
package eu.dnetlib.dhp.oa.dedup;

import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class DedupRecordFactory {

	private static final Logger log = LoggerFactory.getLogger(DedupRecordFactory.class);

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
				(MapFunction<Tuple2<String, T>, String>) entity -> entity._1(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<String, T>, T>) (key,
					values) -> entityMerger(key, values, ts, dataInfo),
				Encoders.bean(clazz));
	}

	private static <T extends OafEntity> T entityMerger(
		String id, Iterator<Tuple2<String, T>> entities, long ts, DataInfo dataInfo) {

		T entity = entities.next()._2();

		final Collection<String> dates = Lists.newArrayList();
		entities
			.forEachRemaining(
				t -> {
					T duplicate = t._2();
					entity.mergeFrom(duplicate);
					if (ModelSupport.isSubClass(duplicate, Result.class)) {
						Result r1 = (Result) duplicate;
						Result er = (Result) entity;
						er.setAuthor(DedupUtility.mergeAuthor(er.getAuthor(), r1.getAuthor()));

						if (r1.getDateofacceptance() != null) {
							dates.add(r1.getDateofacceptance().getValue());
						}
					}
				});

		if (ModelSupport.isSubClass(entity, Result.class)) {
			((Result) entity).setDateofacceptance(DatePicker.pick(dates));
		}

		entity.setId(id);
		entity.setLastupdatetimestamp(ts);
		entity.setDataInfo(dataInfo);

		return entity;
	}
}
