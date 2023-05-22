/*
 * Copyright 2018 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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
package nz.co.gregs.dbvolution.utility;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Regular Processes provide a standard means of performing regular database
 * related tasks.
 *
 * <p>
 * Examples of regular processes are database backups, and clearing old records
 * from tables.</p>
 *
 * <p>
 * Use
 * {@link DBDatabase#addRegularProcess(nz.co.gregs.dbvolution.utility.RegularProcess)}
 * to add a regular process.</p>
 *
 * <p>
 * Regular processes can access the database they are registered with using
 * {@link #getDatabase() }. Accessing more than one database is not supported
 * automatically.</p>
 *
 * <p>
 * The only method you need to implement is {@link #process() } but you can
 * overload {@link #preprocess() }
 * to perform checks before processing and {@link #postprocess() } to clean up
 * any resources.</p>
 *
 * @author gregorygraham
 */
public abstract class RegularProcess implements Serializable {

	public static final long serialVersionUID = 1l;

	final Log LOG = LogFactory.getLog(RegularProcess.class);

	private Instant nextRun = Instant.now();
	ChronoUnit timeField = ChronoUnit.MINUTES;
	private int timeOffset = 5;
	private DBDatabase dbDatabase;
	private String lastResult = "Not Processed Yet";
	private Instant lastRunTime = Instant.now();
	private String simpleName = null;
	private boolean stopped = false;

	/**
	 * Method that does all the processing that needs to be regularly performed.
	 *
	 * <p>
	 * If {@link #preprocess() } returns true, process() is called to perform the
	 * actual processing.
	 *
	 * <p>
	 * {@link #postprocess() } will be called to clean up any resources after
	 * processing and
	 * {@link #handleExceptionDuringProcessing(java.lang.Exception)} can be
	 * overloaded if exceptions during processing need to be handled.
	 *
	 * @return the output from processing
	 * @throws Exception any exception can thrown
	 */
	public abstract String process() throws Exception;

	public final boolean isDueToRun() {
		return hasExceededTimeLimit();
	}

	public final boolean hasExceededTimeLimit() {
		return nextRun.isBefore(Instant.now());
	}

	/**
	 * Sets the time field and offset value to use when generating the next run
	 * time.
	 *
	 * <p>
	 * Note that {@link DBDatabase} regular processes are checked once a minute.
	 *
	 * @param calendarTimeField the time unit to offset the regular process by
	 * @param offset the number time units to offset by
	 */
	public final void setTimeOffset(ChronoUnit calendarTimeField, int offset) {
		timeField = calendarTimeField;
		timeOffset = offset;
	}

	/**
	 * A method that is called before {@link #process() } and will stop processing
	 * if FALSE is returned.
	 *
	 * <p>
	 * By default this method returns true always.
	 *
	 * @return TRUE if processing should continue, FALSE otherwise.
	 */
	public boolean preprocess() {
		return true;
	}

	/**
	 * A method that is always called after {@link #preprocess() } and {@link #process()
	 * }.
	 *
	 * <p>
	 * By default this method does nothing.
	 */
	public void postprocess() {
	}

	/**
	 * Provides a way to intercept exceptions thrown during processing.
	 *
	 * <p>
	 * By default, this method logs the exception as a warning.
	 *
	 * @param ex the exception that has occurred
	 */
	public void handleExceptionDuringProcessing(Exception ex) {
		LOG.warn(this, ex);
	}

	/**
	 * Used to generate the next run time for this process
	 *
	 */
	public final void offsetTime() {
		lastRunTime = Instant.now();
		var duration = Duration.ZERO.plus(timeOffset, timeField);
		Instant instant = Instant.now().plus(duration);
		nextRun = instant;
	}

	/**
	 * Returns the (last) database that this process has been added to using {@link DBDatabase#addRegularProcess(nz.co.gregs.dbvolution.utility.RegularProcess)
	 * }.
	 *
	 * @return the database that this process should work upon.
	 */
	protected final DBDatabase getDatabase() {
		return dbDatabase;
	}

	/**
	 * Used by  {@link DBDatabase#addRegularProcess(nz.co.gregs.dbvolution.utility.RegularProcess) }
	 * to set the database.
	 *
	 * <p>
	 * You probably don't need this method.
	 *
	 * @param db the database this process interacts with
	 */
	public final void setDatabase(DBDatabase db) {
		try {
			this.dbDatabase = db.clone();
		} catch (CloneNotSupportedException ex) {
			Logger.getLogger(RegularProcess.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public final void stop() {
		this.dbDatabase = null;
		this.stopped = true;
	}

	public final boolean canRun() {
		boolean canRun = false;
		if (stopped) {
			LOG.warn("This database is stopped and " + this.getClass().getSimpleName() + " can not proceed.");
		} else if (this.dbDatabase == null) {
			LOG.warn(this.getClass().getSimpleName() + " has not had setDatabase(DBDatabase) called and can not proceed.");
		} else {
			canRun = true;
		}
		return canRun;
	}

	public String getLastResult() {
		return lastResult;
	}

	public Instant getLastRuntime() {
		return lastRunTime;
	}

	public Instant getNextRuntime() {
		return nextRun;
	}

	public void setLastResult(String process) {
		this.lastResult = process;
	}

	public String getSimpleName() {
		if (simpleName == null || simpleName.isEmpty()) {
			return this.getClass().getSimpleName();
		} else {
			return simpleName;
		}
	}

	public void setSimpleName(String simpleName) {
		this.simpleName = simpleName;
	}

	public void clearSimpleName() {
		this.simpleName = null;
	}

}
