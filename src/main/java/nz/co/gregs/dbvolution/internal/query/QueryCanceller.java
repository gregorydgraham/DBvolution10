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
class QueryCanceller implements Runnable {

	private final DBStatement statement;
	private final Date timestamp;
	private final String sql;

	QueryCanceller(DBStatement statement, String sql) {
		this.statement = statement;
		this.sql = sql;
		this.timestamp = new Date();
	}

	@Override
	public void run() {
		try {
			System.out.println("CANCELLER: Cancelling query - "+sql);
			System.out.println("CANCELLER: after ... " + ((0.0+((new Date()).getTime() - timestamp.getTime()))/1000.0) + "seconds");
			statement.cancel();
		} catch (SQLException ex) {
			Logger.getLogger(QueryDetails.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private static Long standardCancelOffset = null;
	private static final int DEFAULT_TIMEOUT_MILLISECONDS = 10000;

	public static Long getStandardCancelOffset() {
		if (standardCancelOffset == null) {
			long targetTicks = 2000000000l;// about 1s worth of ops on the reference platform
			long ticks = 0;
			Date startDate = new Date();
			while (ticks < targetTicks) {
				ticks++;
			}
			standardCancelOffset = Math.max(
					DEFAULT_TIMEOUT_MILLISECONDS, // at least 10s timeout
					((new Date()).getTime() - startDate.getTime()) * 10);// 10x1sec-equivalents
		}
		return standardCancelOffset;
	}

	static final transient ScheduledExecutorService TIMER_SERVICE = Executors.newSingleThreadScheduledExecutor();

	public ScheduledFuture<?> schedule(Long timeoutTimeInMilliseconds) {
		return TIMER_SERVICE.schedule(this, timeoutTimeInMilliseconds, TimeUnit.MILLISECONDS);
	}
}
