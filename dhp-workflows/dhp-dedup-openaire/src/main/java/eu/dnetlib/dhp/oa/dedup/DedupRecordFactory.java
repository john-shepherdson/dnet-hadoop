
package eu.dnetlib.dhp.oa.dedup;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.merge.AuthorMerger;
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
					values) -> entityMerger(key, values, ts, dataInfo, clazz),
				Encoders.bean(clazz));
	}

	public static <T extends OafEntity> T entityMerger(
		String id, Iterator<Tuple2<String, T>> entities, long ts, DataInfo dataInfo, Class<T> clazz)
		throws IllegalAccessException, InstantiationException {

		T entity = clazz.newInstance();

		final Collection<String> dates = Lists.newArrayList();
		final List<List<Author>> authors = Lists.newArrayList();
		final List<Identifier<T>> bestPids = Lists.newArrayList(); // best pids list

		entities
			.forEachRemaining(
				t -> {
					T duplicate = t._2();

					// prepare the list of pids to be used for the id generation
					bestPids.add(Identifier.newInstance(duplicate));

					entity.mergeFrom(duplicate);
					if (ModelSupport.isSubClass(duplicate, Result.class)) {
						Result r1 = (Result) duplicate;
						if (r1.getAuthor() != null && r1.getAuthor().size() > 0)
							authors.add(r1.getAuthor());
						if (r1.getDateofacceptance() != null)
							dates.add(r1.getDateofacceptance().getValue());
					}

				});

		// set authors and date
		if (ModelSupport.isSubClass(entity, Result.class)) {
			((Result) entity).setDateofacceptance(DatePicker.pick(dates));
			((Result) entity).setAuthor(AuthorMerger.merge(authors));
		}

		entity.setId(IdGenerator.generate(bestPids, id));

		entity.setLastupdatetimestamp(ts);
		entity.setDataInfo(dataInfo);

		return entity;
	}

}
