
package eu.dnetlib.dhp.oa.dedup;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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
		Dataset<Row> entities = spark
			.read()
			.schema(Encoders.bean(clazz).schema())
			.json(entitiesInputPath)
			.as(Encoders.bean(clazz))
			.map(
				(MapFunction<T, Tuple2<String, T>>) entity -> {
					return new Tuple2<>(entity.getId(), entity);
				},
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)))
			.selectExpr("_1 AS id", "_2 AS kryoObject");

		// <source, target>: source is the dedup_id, target is the id of the mergedIn
		Dataset<Row> mergeRels = spark
			.read()
			.load(mergeRelsInputPath)
			.where("relClass == 'merges'")
			.selectExpr("source as dedupId", "target as id");

		return mergeRels
			.join(entities, "id")
			.select("dedupId", "kryoObject")
			.as(Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)))
			.groupByKey((MapFunction<Tuple2<String, T>, String>) Tuple2::_1, Encoders.STRING())
			.reduceGroups(
				(ReduceFunction<Tuple2<String, T>>) (t1, t2) -> new Tuple2<>(t1._1(),
					reduceEntity(t1._1(), t1._2(), t2._2(), clazz)))
			.map(
				(MapFunction<Tuple2<String, Tuple2<String, T>>, T>) t -> {
					T res = t._2()._2();
					res.setDataInfo(dataInfo);
					res.setLastupdatetimestamp(ts);
					return res;
				},
				Encoders.bean(clazz));
	}

	public static <T extends OafEntity> T reduceEntity(
		String id, T entity, T duplicate, Class<T> clazz) {

		int compare = new IdentifierComparator()
			.compare(Identifier.newInstance(entity), Identifier.newInstance(duplicate));

		if (compare > 0) {
			T swap = duplicate;
			duplicate = entity;
			entity = swap;
		}

		entity.mergeFrom(duplicate);
		entity.setId(id);

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
		String id, Iterator<Tuple2<String, T>> entities, long ts, DataInfo dataInfo, Class<T> clazz)
		throws IllegalAccessException, InstantiationException, InvocationTargetException {
		T base = entities.next()._2();

		while (entities.hasNext()) {
			T duplicate = entities.next()._2();
			if (duplicate != null)
				base = reduceEntity(id, base, duplicate, clazz);
		}

		base.setDataInfo(dataInfo);
		base.setLastupdatetimestamp(ts);

		return base;
	}

}
