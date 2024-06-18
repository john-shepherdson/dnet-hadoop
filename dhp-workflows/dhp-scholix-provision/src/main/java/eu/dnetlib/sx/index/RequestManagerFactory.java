
package eu.dnetlib.sx.index;

public class RequestManagerFactory {
	public static RequestManager fromType(final String type) {
		if ("scholix".equalsIgnoreCase(type))
			return new ScholixRequestManager();
		if ("summary".equalsIgnoreCase(type))
			return new SummaryRequestManager();
		throw new IllegalArgumentException("unexpected type");
	}

}
