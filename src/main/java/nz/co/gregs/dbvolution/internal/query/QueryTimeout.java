/*
 * Copyright 2017 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.internal.query;

import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBStatement;

/**
 *
 * @author gregorygraham
 */
public class QueryTimeout {

	protected static final Logger LOGGER = Logger.getLogger(QueryTimeout.class.getName());
	static final transient ScheduledExecutorService TIMER_SERVICE = Executors.newSingleThreadScheduledExecutor();

	private final DBStatement statement;
	private final Date timestamp;
	private final String sql;
	private boolean timeoutOccured = false;
	private StatementDetails details = null;
	private boolean stillRequired = true;
	private static Long standardTimeoutOffset = null;
	private static final long DEFAULT_TIMEOUT_MILLISECONDS = 15000L;
	private ScheduledFuture<?> timeoutHandler;
	private final TimeOut timeout = new TimeOut();

	public QueryTimeout(StatementDetails details, Long timeoutTime) {
		this.details = details;
		this.statement = details.getDBStatement();
		this.sql = details.getSql();
		this.timestamp = new Date();
		scheduleIfRequired(timeoutTime);
	}

	private void scheduleIfRequired(Long timeoutTime) {
		// special cases first
		if (timeoutTime == null || timeoutTime == 0L) {
			// null or zero is not a valid timeout value, use the default instead
			scheduleOnTimerService(DEFAULT_TIMEOUT_MILLISECONDS);
			return;
		}
		if (timeoutTime < 0) {
			// negative implies no timeout
			this.timeoutHandler = null;
			return;
		}
		// not a special case so proceed
		scheduleOnTimerService(timeoutTime);
	}

	private void scheduleOnTimerService(Long timeoutTimeInMilliseconds) {
		timeoutHandler = TIMER_SERVICE.schedule(timeout, timeoutTimeInMilliseconds, TimeUnit.MILLISECONDS);
	}

	public static Long getStandardTimeoutOffset() {
		if (standardTimeoutOffset == null) {
			long targetTicks = 2000l;
			long ticks = 0;
			Date startDate = new Date();
			while (ticks < targetTicks) {
				ticks++;
			}
			standardTimeoutOffset = Math.max(
					DEFAULT_TIMEOUT_MILLISECONDS, // at least 10s timeout
					((new Date()).getTime() - startDate.getTime()) * 15);// 15x1sec-equivalents
		}
		return standardTimeoutOffset;
	}

	public synchronized boolean queryTimedOut() {
		return timeoutOccured;
	}

	public synchronized void noLongerRequired() {
		stillRequired = false;
		if (timeoutHandler != null) {
			timeoutHandler.cancel(true);
		}
	}

	private class TimeOut implements Runnable {

		@Override
		public void run() {
			if (stillRequired) {
				try {
					if (details != null && !details.isIgnoreExceptions()) {
						System.out.format("TIMEOUT: Cancelling query after {0} seconds {1} => {2}",
								(0.0 + ((new Date()).getTime() - timestamp.getTime())) / 1000.0,
								details.getLabel(),
								sql);
						LOGGER.log(
								Level.WARNING,
								"TIMEOUT: Cancelling query after {0} seconds {1} => {2}",
								new Object[]{
									(0.0 + ((new Date()).getTime() - timestamp.getTime())) / 1000.0,
									details.getLabel(),
									sql
								});
					}
					statement.cancel();
				} catch (SQLException ex) {
					Logger.getLogger(QueryDetails.class.getName()).log(Level.SEVERE, "QueryCanceller caught an exception", ex);
				} finally {
					timeoutOccured = true;
				}
			}
		}

	}
}
