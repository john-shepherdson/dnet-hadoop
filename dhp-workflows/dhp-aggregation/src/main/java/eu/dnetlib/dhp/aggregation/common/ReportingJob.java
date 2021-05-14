
package eu.dnetlib.dhp.aggregation.common;

import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class ReportingJob {

	/**
	 * Frequency (seconds) for sending ongoing messages to report the collection task advancement
	 */
	public static final int ONGOING_REPORT_FREQUENCY = 5;

	/**
	 * Initial delay (seconds) for sending ongoing messages to report the collection task advancement
	 */
	public static final int INITIAL_DELAY = 2;

	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	protected final AggregatorReport report;

	public ReportingJob(AggregatorReport report) {
		this.report = report;
	}

	protected void schedule(final ReporterCallback callback) {
		executor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				report.ongoing(callback.getCurrent(), callback.getTotal());
			}
		}, INITIAL_DELAY, ONGOING_REPORT_FREQUENCY, TimeUnit.SECONDS);
	}

	protected void shutdown() {
		executor.shutdown();
	}
}
