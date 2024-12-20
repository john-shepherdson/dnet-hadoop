
package eu.dnetlib.dhp.collection.plugin.zenodo;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class ZenodoTarIterator implements Iterator<String>, Closeable {

	private final InputStream gzipInputStream;
	private final StringBuilder currentItem = new StringBuilder();
	private TarArchiveInputStream tais;
	private boolean hasNext;

	public ZenodoTarIterator(InputStream gzipInputStream) {
		this.gzipInputStream = gzipInputStream;
		tais = new TarArchiveInputStream(gzipInputStream);
		hasNext = getNextItem();
	}

	private boolean getNextItem() {
		try {
			TarArchiveEntry entry;
			while ((entry = tais.getNextTarEntry()) != null) {
				if (entry.isFile()) {
					currentItem.setLength(0);
					currentItem.append(IOUtils.toString(new InputStreamReader(tais)));
					return true;
				}
			}
			return false;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public String next() {
		final String data = currentItem.toString();
		hasNext = getNextItem();
		return data;
	}

	@Override
	public void close() throws IOException {
		gzipInputStream.close();
	}
}
