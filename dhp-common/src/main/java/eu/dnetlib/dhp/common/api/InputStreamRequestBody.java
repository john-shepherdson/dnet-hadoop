
package eu.dnetlib.dhp.common.api;

import java.io.IOException;
import java.io.InputStream;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

public class InputStreamRequestBody extends RequestBody {

	private InputStream inputStream;
	private MediaType mediaType;
	private long lenght;

	public static RequestBody create(final MediaType mediaType, final InputStream inputStream, final long len) {

		return new InputStreamRequestBody(inputStream, mediaType, len);
	}

	private InputStreamRequestBody(InputStream inputStream, MediaType mediaType, long len) {
		this.inputStream = inputStream;
		this.mediaType = mediaType;
		this.lenght = len;
	}

	@Override
	public MediaType contentType() {
		return mediaType;
	}

	@Override
	public long contentLength() {

		return lenght;

	}

	@Override
	public void writeTo(BufferedSink sink) throws IOException {
		Source source = null;
		try {
			source = Okio.source(inputStream);
			sink.writeAll(source);
		} finally {
			Util.closeQuietly(source);
		}
	}
}
