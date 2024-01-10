
package eu.dnetlib.dhp.collection.orcid;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.dnetlib.dhp.common.collection.HttpClientParams;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

public class OrcidGetUpdatesFile {

    private static Logger log = LoggerFactory.getLogger(OrcidGetUpdatesFile.class);

    public static void main(String[] args) throws IOException {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                "/eu/dnetlib/dhp/collection/orcid/download_orcid_update_parameter.json");

        final String namenode = parser.get("namenode");
        log.info("got variable namenode: {}", namenode);

        final String targetPath = parser.get("targetPath");
        log.info("got variable targetPath: {}", targetPath);


        final String apiURL = parser.get("apiURL");
        log.info("got variable apiURL: {}", apiURL);

		final String accessToken = parser.get("accessToken");
		log.info("got variable accessToken: {}", accessToken);



        final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(namenode));


    }

	private SequenceFile.Writer createFile(Path aPath, FileSystem fileSystem) throws IOException {
		return  SequenceFile
				.createWriter(
						fileSystem.getConf(),
						SequenceFile.Writer.file(aPath),
						SequenceFile.Writer.keyClass(Text.class),
						SequenceFile.Writer.valueClass(Text.class));
	}


    private ORCIDWorker createWorker(final String id, final String targetPath, final BlockingQueue<String> queue, final String accessToken, FileSystem fileSystem) throws Exception {
		return ORCIDWorker.builder()
				.withId(id)
				.withEmployments(createFile(new Path(String.format("%s/employments_%s", targetPath, id)), fileSystem))
				.withSummary(createFile(new Path(String.format("%s/summary_%s", targetPath, id)), fileSystem))
				.withWorks(createFile(new Path(String.format("%s/works_%s", targetPath, id)), fileSystem))
				.withAccessToken(accessToken)
				.withBlockingQueue(queue)
				.build();
    }



    public void readTar(FileSystem fileSystem, final String accessToken, final String apiURL, final String targetPath, final String startDate ) throws Exception {
		final HttpURLConnection urlConn = (HttpURLConnection) new URL(apiURL).openConnection();
		final HttpClientParams clientParams = new HttpClientParams();
		urlConn.setInstanceFollowRedirects(false);
		urlConn.setReadTimeout(clientParams.getReadTimeOut() * 1000);
		urlConn.setConnectTimeout(clientParams.getConnectTimeOut() * 1000);
		if (urlConn.getResponseCode()>199 && urlConn.getResponseCode()<300) {
			InputStream input = urlConn.getInputStream();
			TarArchiveInputStream tais = new TarArchiveInputStream(new GzipCompressorInputStream(
					new BufferedInputStream(
							input)));
			TarArchiveEntry entry;

			BlockingQueue<String> queue = new ArrayBlockingQueue<String>(3000);
			final List<ORCIDWorker> workers = new ArrayList<>();
			for (int i = 0; i <20; i++) {
				workers.add(createWorker(""+i,targetPath,queue,accessToken, fileSystem));
			}
			workers.forEach(Thread::start);


            while ((entry = tais.getNextTarEntry()) != null) {

				if (entry.isFile()) {

					BufferedReader br = new BufferedReader(new InputStreamReader(tais));
					System.out.println(br.readLine());
					br.lines().map(l -> l.split(",")).filter(s -> StringUtils.compare(s[3].substring(0, 10), startDate) > 0).map(s->s[0]).limit(200).forEach(s -> {
                        try {
							log.info("Adding item ");
                            queue.put(s);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
					queue.put(ORCIDWorker.JOB_COMPLETE);

				}
			}

			for (ORCIDWorker worker : workers) {
				worker.join();
			}


		}





    }
}
