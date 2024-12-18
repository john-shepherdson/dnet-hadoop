/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.common.FunctionalInterfaceSupport.*;
import static eu.dnetlib.dhp.schema.common.ModelSupport.isSubClass;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class PromoteResultWithMeasuresTest {

	private static final Logger log = LoggerFactory.getLogger(PromoteResultWithMeasuresTest.class);

	private static SparkSession spark;

	private static Path tempDir;

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@BeforeAll
	public static void beforeAll() throws IOException {
		tempDir = Files.createTempDirectory(PromoteResultWithMeasuresTest.class.getSimpleName());
		log.info("using work dir {}", tempDir);

		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName(PromoteResultWithMeasuresTest.class.getSimpleName());
		conf.set("spark.driver.host", "localhost");

		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");

		conf.set("spark.sql.warehouse.dir", tempDir.toString());
		conf.set("hive.metastore.warehouse.dir", tempDir.resolve("warehouse").toString());

		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		spark.stop();
		FileUtils.deleteDirectory(tempDir.toFile());
	}

	@Test
	void testPromoteResultWithMeasures_job() throws Exception {

		final String inputGraphTablePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/promote/measures/graph")
			.getPath();

		final String inputActionPayloadPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/promote/measures/actionPayloads")
			.getPath();

		final String actionPayloadsPath = tempDir.resolve("actionPayloads").toString();

		spark
			.read()
			.text(inputActionPayloadPath)
			.withColumn("payload", col("value"))
			.select("payload")
			.write()
			.parquet(actionPayloadsPath);

		final Path outputGraphTablePath = tempDir.resolve("outputGraphTablePath");

		PromoteActionPayloadForGraphTableJob
			.main(new String[] {
				"--isSparkSessionManaged", Boolean.FALSE.toString(),
				"--graphTableClassName", Publication.class.getCanonicalName(),
				"--inputGraphTablePath", inputGraphTablePath,
				"--inputActionPayloadPath", actionPayloadsPath,
				"--actionPayloadClassName", Result.class.getCanonicalName(),
				"--outputGraphTablePath", outputGraphTablePath.toString(),
				"--mergeAndGetStrategy", MergeAndGet.Strategy.MERGE_FROM_AND_GET.toString(),
				"--promoteActionStrategy", PromoteAction.Strategy.ENRICH.toString(),
				"--shouldGroupById", "true"
			});

		assertFalse(isDirEmpty(outputGraphTablePath));

		final Encoder<Publication> pubEncoder = Encoders.bean(Publication.class);
		List<Publication> results = spark
			.read()
			.schema(pubEncoder.schema())
			.json(outputGraphTablePath.toString())
			.as(pubEncoder)
			.collectAsList();

		verify(results);
	}

	@Test
	void testPromoteResultWithMeasures_internal() throws JsonProcessingException {

		Dataset<Publication> rowDS = spark
			.read()
			.schema(Encoders.bean(Publication.class).schema())
			.json("src/test/resources/eu/dnetlib/dhp/actionmanager/promote/measures/graph")
			.as(Encoders.bean(Publication.class));

		Dataset<Result> actionPayloadDS = spark
			.read()
			.schema(Encoders.bean(Result.class).schema())
			.json("src/test/resources/eu/dnetlib/dhp/actionmanager/promote/measures/actionPayloads")
			.as(Encoders.bean(Result.class));

		final MergeAndGet.Strategy mergeFromAndGet = MergeAndGet.Strategy.MERGE_FROM_AND_GET;

		final SerializableSupplier<Function<Publication, String>> rowIdFn = ModelSupport::idFn;
		final SerializableSupplier<BiFunction<Publication, Result, Publication>> mergeAndGetFn = MergeAndGet
			.functionFor(mergeFromAndGet);
		final SerializableSupplier<Publication> zeroFn = () -> Publication.class
			.cast(new eu.dnetlib.dhp.schema.oaf.Publication());
		final SerializableSupplier<Function<Publication, Boolean>> isNotZeroFn = PromoteResultWithMeasuresTest::isNotZeroFnUsingIdOrSourceAndTarget;

		Dataset<Publication> joinedResults = PromoteActionPayloadFunctions
			.joinGraphTableWithActionPayloadAndMerge(
				rowDS,
				actionPayloadDS,
				rowIdFn,
				ModelSupport::idFn,
				mergeAndGetFn,
				PromoteAction.Strategy.ENRICH,
				Publication.class,
				Result.class);

		SerializableSupplier<BiFunction<Publication, Publication, Publication>> mergeRowsAndGetFn = MergeAndGet
			.functionFor(mergeFromAndGet);

		Dataset<Publication> mergedResults = PromoteActionPayloadFunctions
			.groupGraphTableByIdAndMerge(
				joinedResults, rowIdFn, mergeRowsAndGetFn, zeroFn, isNotZeroFn, Publication.class);

		verify(mergedResults.collectAsList());
	}

	private static void verify(List<Publication> results) throws JsonProcessingException {
		assertNotNull(results);
		assertEquals(1, results.size());

		Result r = results.get(0);

		log.info(OBJECT_MAPPER.writeValueAsString(r));

		assertNotNull(r.getMeasures());
		assertFalse(r.getMeasures().isEmpty());
		assertTrue(
			r
				.getMeasures()
				.stream()
				.map(Measure::getId)
				.collect(Collectors.toCollection(HashSet::new))
				.containsAll(
					Lists
						.newArrayList(
							"downloads", "views", "influence", "popularity", "influence_alt", "popularity_alt",
							"impulse")));
	}

	private static <T extends Oaf> Function<T, Boolean> isNotZeroFnUsingIdOrSourceAndTarget() {
		return t -> {
			if (isSubClass(t, Relation.class)) {
				final Relation rel = (Relation) t;
				return StringUtils.isNotBlank(rel.getSource()) && StringUtils.isNotBlank(rel.getTarget());
			}
			return StringUtils.isNotBlank(((OafEntity) t).getId());
		};
	}

	private static boolean isDirEmpty(final Path directory) throws IOException {
		try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
			return !dirStream.iterator().hasNext();
		}
	}

}
