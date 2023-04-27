
package eu.dnetlib.dhp.sx;

import static eu.dnetlib.dhp.sx.provision.DropAndCreateESIndex.APPLICATION_JSON;
import static eu.dnetlib.dhp.sx.provision.DropAndCreateESIndex.STATUS_CODE_TEXT;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.sx.scholix.Scholix;
import eu.dnetlib.dhp.schema.sx.scholix.ScholixFlat;
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils;
import eu.dnetlib.dhp.sx.provision.DropAndCreateESIndex;
import eu.dnetlib.dhp.sx.provision.SparkIndexCollectionOnES;

public class FlatIndexTest {
	@Test
	public void dropAndCreateIndex() {

		Logger log = LoggerFactory.getLogger(getClass().getName());

		final String url = "http://localhost:9200/dli_scholix";

		try (CloseableHttpClient client = HttpClients.createDefault()) {

			HttpDelete delete = new HttpDelete(url);

			CloseableHttpResponse response = client.execute(delete);

			log.info("deleting Index SCHOLIX");
			log.info(STATUS_CODE_TEXT, response.getStatusLine());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		try (CloseableHttpClient client = HttpClients.createDefault()) {

			final String scholixConf = IOUtils
				.toString(
					Objects
						.requireNonNull(
							DropAndCreateESIndex.class
								.getResourceAsStream("/eu/dnetlib/dhp/sx/provision/scholix_index_flat.json")));

			log.info("creating Index SCHOLIX");
			final HttpPut put = new HttpPut(url);

			final StringEntity entity = new StringEntity(scholixConf);
			put.setEntity(entity);
			put.setHeader("Accept", APPLICATION_JSON);
			put.setHeader("Content-type", APPLICATION_JSON);

			final CloseableHttpResponse response = client.execute(put);
			log.info(STATUS_CODE_TEXT, response.getStatusLine());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String compressString(final String input) {
		try {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			Base64OutputStream b64os = new Base64OutputStream(os);
			GZIPOutputStream gzip = new GZIPOutputStream(b64os);
			gzip.write(input.getBytes(StandardCharsets.UTF_8));
			gzip.close();
			b64os.close();
			return new String(os.toByteArray(), StandardCharsets.UTF_8);
		} catch (Throwable t) {
			t.printStackTrace();
			return null;
		}
	}

	private String uncompress(final String compressed) throws Exception {
		Base64InputStream bis = new Base64InputStream(new ByteArrayInputStream(compressed.getBytes()));
		GZIPInputStream gzip = new GZIPInputStream(bis);
		return IOUtils.toString(gzip);
	}

	public void testFeedIndex() throws Exception {

		InputStream gzipStream = new GZIPInputStream(
			getClass().getResourceAsStream("/eu/dnetlib/dhp/sx/provision/scholix_dump.gz"));
		Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
		BufferedReader buffered = new BufferedReader(decoder);
		final ObjectMapper mapper = new ObjectMapper();
		GZIPOutputStream gzip = new GZIPOutputStream(Files.newOutputStream(Paths.get("/tmp/scholix_flat.gz")));
		String line = buffered.readLine();
		while (line != null) {

			final Scholix s = mapper.readValue(line, Scholix.class);
			final ScholixFlat flat = ScholixUtils.flattenizeScholix(s, compressString(line));
			gzip.write(mapper.writeValueAsString(flat).concat("\n").getBytes(StandardCharsets.UTF_8));
			line = buffered.readLine();
		}
		gzip.close();

		SparkConf conf = new SparkConf()
			.setAppName(SparkIndexCollectionOnES.class.getSimpleName())
			.setMaster("local[*]");
		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		try (final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

			JavaRDD<String> inputRdd = sc.textFile("/tmp/scholix_flat.gz");

			Map<String, String> esCfg = new HashMap<>();
			esCfg.put("es.nodes", "localhost");
			esCfg.put("es.mapping.id", "identifier");
			esCfg.put("es.batch.write.retry.count", "8");
			esCfg.put("es.batch.write.retry.wait", "60s");
			esCfg.put("es.batch.size.entries", "200");
			esCfg.put("es.nodes.wan.only", "true");
			JavaEsSpark.saveJsonToEs(inputRdd, "scholix", esCfg);
		}
	}
}
