package eu.dnetlib.dhp.oa.oaipmh;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Test;

class IrishOaiExporterJobTest {

	@Test
	void testAsIrishOaiResult() throws Exception {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("record_IE.xml"));
		final OaiRecordWrapper res = IrishOaiExporterJob.asIrishOaiResult(xml);
		assertNotNull(res.getId());
		assertNotNull(res.getBody());
		assertNotNull(res.getSets());
		assertNotNull(res.getDate());
		assertEquals("dedup_wf_002::532be02f990b479a1da46d71f1a4c3f0", res.getId());
		assertTrue(res.getBody().length > 0);
		assertTrue(res.getSets().isEmpty());
	}

	@Test
	void testIsValid_IE() throws DocumentException {
		final Document doc = new SAXReader().read(getClass().getResourceAsStream("record_IE.xml"));
		assertTrue(IrishOaiExporterJob.isValid(doc));
	}

	@Test
	void testIsValid_invalid_country() throws DocumentException {
		final Document doc = new SAXReader().read(getClass().getResourceAsStream("record_IT.xml"));
		assertFalse(IrishOaiExporterJob.isValid(doc));
	}

	@Test
	void testIsValid_deleted() throws DocumentException {
		final Document doc = new SAXReader().read(getClass().getResourceAsStream("record_IE_deleted.xml"));
		assertFalse(IrishOaiExporterJob.isValid(doc));
	}

	@Test
	void testGzip_simple() {
		final String message = "<test />";
		final byte[] bytes = IrishOaiExporterJob.gzip(message);
		assertNotNull(bytes);
		assertTrue(bytes.length > 0);
		assertEquals(message, decompress(bytes));
	}

	@Test
	void testGzip_empty() {
		assertNull(IrishOaiExporterJob.gzip(""));
		assertNull(IrishOaiExporterJob.gzip(null));
	}

	private static String decompress(final byte[] compressed) {
		final StringBuilder outStr = new StringBuilder();
		if ((compressed == null) || (compressed.length == 0)) { return null; }
		try {
			if (isCompressed(compressed)) {
				final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
				final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

				String line;
				while ((line = bufferedReader.readLine()) != null) {
					outStr.append(line);
				}
			} else {
				outStr.append(compressed);
			}
			return outStr.toString();
		} catch (final IOException e) {
			throw new RuntimeException("error in gunzip", e);
		}
	}

	private static boolean isCompressed(final byte[] compressed) {
		return (compressed[0] == (byte) GZIPInputStream.GZIP_MAGIC) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
	}
}