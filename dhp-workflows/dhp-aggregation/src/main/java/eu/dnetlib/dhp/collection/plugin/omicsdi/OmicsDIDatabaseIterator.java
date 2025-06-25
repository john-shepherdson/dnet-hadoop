
package eu.dnetlib.dhp.collection.plugin.omicsdi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang3.math.NumberUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OmicsDIDatabaseIterator implements Iterator<String> {

	private final HttpClientParams clientParams;

	private final String baseUrl;
	private final String repo;
	private final int pageSize;

	private int currStart = -1;
	private int estimatedTotal = Integer.MAX_VALUE;

	private final Queue<String> queue = new LinkedList<>();

	private String nextElement;

	private static final Logger log = LoggerFactory.getLogger(OmicsDIDatabaseIterator.class);

	public OmicsDIDatabaseIterator(final String baseUrl, final String repo, final int pageSize,
		final HttpClientParams clientParams) {
		this.baseUrl = baseUrl;
		this.repo = repo;
		this.pageSize = pageSize;

		this.clientParams = clientParams;

		prepareNextElement();
	}

	@Override
	public boolean hasNext() {
		return this.nextElement != null;
	}

	@Override
	public String next() {
		try {
			return this.nextElement;
		} finally {
			prepareNextElement();
		}
	}

	private void prepareNextElement() {
		if (this.queue.isEmpty()) {
			if (this.currStart < 0) {
				this.queue.addAll(nextPage(0));
			} else if ((this.currStart + this.pageSize) < this.estimatedTotal) {
				this.queue.addAll(nextPage(this.currStart + this.pageSize));
			}
		}

		this.nextElement = this.queue.poll();
	}

	private List<String> nextPage(final int start) {
		this.currStart = start;

		final String url = databaseUrl(this.currStart);

		log.info("DOWNLOADING URL: " + url);

		final HttpConnector2 connector = new HttpConnector2(this.clientParams);
		final List<String> res = new ArrayList<>();
		try {
			final String json = connector.getInputSource(url);
			final String xml = JsonUtils.convertToXML(json);
			final Document doc = DocumentHelper.parseText(xml);

			this.estimatedTotal = NumberUtils.toInt(doc.valueOf("/recordWrap/count"));

			for (final Object o : doc.selectNodes("/recordWrap/datasets")) {
				final Element e = (Element) o;
				e.addElement("databaseRepo").addText(this.repo);
				res.add(e.asXML());
			}
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}

		return res;
	}

	private String databaseUrl(final int startPage) {

		final String escapedRepo = this.repo.contains(" ") ? "%22" + this.repo.replace(" ", "%20") + "%22" : this.repo;

		return String
			.format(
				"%s/dataset/search?sort_field=id&query=repository:%s&size=%d&start=%d&format=json", this.baseUrl,
				escapedRepo, this.pageSize, startPage);
	}

}
