public package eu.dnetlib.dhp.collection.plugin;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import eu.dnetlib.data.collector.rmi.CollectorServiceException;

public class FileGZipCollectorPlugin extends AbstractSplittedRecordPlugin {

	@Override
	protected BufferedInputStream getBufferedInputStream(final String baseUrl) throws CollectorServiceException {

		try {
			GZIPInputStream stream = new GZIPInputStream(new FileInputStream(new URL(baseUrl).getPath()));
			return new BufferedInputStream(stream);
		} catch (Exception e) {
			throw new CollectorServiceException(e);
		}
	}

}
