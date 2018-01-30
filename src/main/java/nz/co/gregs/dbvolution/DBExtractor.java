/*
 * Copyright 2015 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;

/**
 * Extractor to retrieve data from unreliable or resource constrained databases,
 * or from exceptionally large queries.
 *
 * <p>
 * To use Extractor, create a subclass implementing the
 * {@link #getQuery(nz.co.gregs.dbvolution.databases.DBDatabase, int, int)} and
 * {@link #processRows(java.util.List)} methods and call {@link #extract() }.
 * <p>
 * The extractor uses primary key ranges to reduce the size of the query to
 * something the database can handle. The range is increased or decreased
 * automatically depending on whether database coped with the request well or
 * not.
 *
 * <p>
 * All values will be returned, unless the database failed to process a range of
 * 1 (that is a single row) which case the value is skipped and the Extractor
 * will continue.
 *
 * <p>
 * A key feature of using Extractor over other methods is its ability to
 * accelerate and brake as possible or required to achieve close to optimal
 * throughput.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public abstract class DBExtractor extends DBScript {

	/*
	 * To change this license header, choose License Headers in Project Properties.
	 * To change this template file, choose Tools | Templates
	 * and open the template in the editor.
	 */
	private int maxBoundIncrease = 10000000;
	private static final int MIN_BOUND_INCREASE = 1;
	private int boundIncrease = 10;

	private int maxBound = 200 * 1000000;
	private int startLowerBound = 0;
	private int lowerBound = 0;
	private boolean moreRecords = true;
	private double previousTimePerRecord = Double.MAX_VALUE; // ridiculous default is only to seed the process.
	private Integer timeoutInMilliseconds = 10000;
	private Long rowCount = null;
	private boolean countOnly = false;
	private final DBDatabase database;

	/**
	 * Default constructor.
	 *
	 * @param db
	 */
	public DBExtractor(DBDatabase db) {
		database = db;
	}

	/**
	 * When Extractor has successfully extract some rows, they are handed to this
	 * method for processing.
	 *
	 * @param rows
	 * @throws Exception
	 */
	abstract public void processRows(List<DBQueryRow> rows) throws Exception;

	/**
	 * Using the database and bounds provided, construct the required query.
	 *
	 * <p>
	 * Extractor does not know the query you want executed so this is the place to
	 * add it.
	 *
	 * <p>
	 * Choose one important table in your query and add the lower- and
	 * upper-bounds provided to the primary key as a permitted range:<br>
	 * {@code employee.employeeID.permittedRange(lowerbound, upperbound);}
	 *
	 * <p>
	 * Add the table to your query and return it to the extractor process:<br>
	 * {@code return db.getDBQuery(employee);}
	 *
	 * <p>
	 * The rows found by the Extractor will be sent to
	 * {@link #processRows(java.util.List)} .
	 *
	 * @param db
	 * @param lowerbound
	 * @param upperbound
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a DBQuery for the database and range
	 */
	abstract public DBQuery getQuery(DBDatabase db, int lowerbound, int upperbound);

	private DBDatabase getDatabase() {
		return database;
	}

	/**
	 * Starts the extraction process.
	 *
	 * <p>
	 * Call this method in your Extractor subclass to start extracting rows from
	 * the database and processing them.
	 *
	 * <p>
	 * Works in conjuction with the
	 * {@link #getQuery(nz.co.gregs.dbvolution.databases.DBDatabase, int, int)}
	 * and {@link #processRows(java.util.List)} method to provide a dynamic
	 * extraction process that achieves fast results on unreliable or
	 * under-resourced databases.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return @throws Exception
	 */
	public final DBActionList extract() throws Exception {
		DBActionList actions = new DBActionList();
		DBDatabase db = getDatabase();
		startLowerBound = lowerBound;
		Date startTime = new Date();
		GregorianCalendar cal = new GregorianCalendar();
		while (hasMoreRecords()) {
			actions.addAll(db.test(this));
			Date finishTime = new Date();
			final double elapsedTimeInMilliseconds = 0.0 + finishTime.getTime() - startTime.getTime();
			double timePerRecord = elapsedTimeInMilliseconds / (lowerBound - startLowerBound);
			System.out.println("EXTRACTED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") in " + elapsedTimeInMilliseconds + "ms at " + timePerRecord + "ms/record.");
			double estimatedRequiredTime = timePerRecord * (maxBound - startLowerBound);
			cal.setTime(startTime);
			int secondsValue = (int)(estimatedRequiredTime / 1000.0D);
			cal.add(Calendar.SECOND, secondsValue);
			double timeInHours = (Math.round((estimatedRequiredTime / (1000 * 60 * 60)) * 100) + 0.0) / 100.0;
			double timeInMinutes = (Math.round((estimatedRequiredTime / (1000 * 60)) * 100) + 0.0) / 100.0;
			double elapsedTimeInMinutes = (Math.round((elapsedTimeInMilliseconds / (1000 * 60)) * 100) + 0.0) / 100.0;
			double elapsedTimeInHours = (Math.round((elapsedTimeInMilliseconds / (1000 * 60 * 60)) * 100) + 0.0) / 100.0;
			double remainingTimeInMinutes = timeInMinutes - elapsedTimeInMinutes;
			double remainingTimeInHours = timeInHours - elapsedTimeInHours;
			if (timeInHours > 1) {
				System.out.println("PROJECTED: time=" + timeInHours + "hours: " + (cal.getTime()));
				System.out.println("ELAPSED: time=" + elapsedTimeInHours + "hours");
				System.out.println("REMAINING: time=" + remainingTimeInHours + "hours");
			} else {
				System.out.println("PROJECTED: time=" + timeInMinutes + "minutes: " + (cal.getTime()));
				System.out.println("ELAPSED: time=" + elapsedTimeInMinutes + "minutes");
				System.out.println("REMAINING: time=" + remainingTimeInMinutes + "minutes");
			}
		}
		return actions;
	}

	/**
	 * Used to maintain the process in isolation from all other processes and
	 * ensure that the processing does not alter any rows.
	 *
	 * <p>
	 * This method cannot be changed.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an action list
	 * @throws java.io.FileNotFoundException
	 * @throws java.sql.SQLException
	 */
	@Override
	public final DBActionList script(DBDatabase db) throws FileNotFoundException, IOException, SQLException, Exception {
		DBActionList actions = new DBActionList();

		List<DBQueryRow> rows = getRows(db);
		Date startTime = new Date();
		processRows(rows);
		Date finishTime = new Date();
		double timePerRecord = (0.0 + finishTime.getTime() - startTime.getTime()) / getBoundIncrease();
		System.out.println("PROCESSED: " + (getLowerBound()) + "-" + (getUpperBound() - getBoundIncrease()) + " (+" + getBoundIncrease() + ") at " + timePerRecord + "ms/record.");

		return actions;
	}

	@SuppressFBWarnings(
			value = "REC_CATCH_EXCEPTION", 
			justification = "Database vendors throw many interesting exceptions")
	private List<DBQueryRow> getRows(DBDatabase db) throws AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<DBQueryRow> rows = null;
		this.rowCount = 0L;
		double timePerRecord = 10000.0;
		while (hasMoreRecords() && rows == null) {
			try {
				if (getLowerBound() > getMaxBound()) {
					setMoreRecords(false);
				} else {
					System.out.println("RETRIEVING: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ")");
					DBQuery dbQuery = getQuery(db, getLowerBound(), getUpperBound());
					setQueryTimeout(dbQuery);
					Date startTime = new Date();
					if (this.countOnly) {
						rowCount = dbQuery.count();
						rows = new ArrayList<>();
					} else {
						rows = dbQuery.getAllRows();
						rowCount = 0L + rows.size();
					}
					Date finishTime = new Date();
					final double timeTaken = 0.0 + finishTime.getTime() - startTime.getTime();
					timePerRecord = timeTaken / getBoundIncrease();
					System.out.println("RETRIEVED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") after " + timeTaken + " at " + timePerRecord + "ms/record.");
				}
			} catch (SQLException | AccidentalBlankQueryException | AccidentalCartesianJoinException ex) {
				if (getBoundIncrease() == getMinBoundIncrease()) {
					// We can't get this row so acknowledge the error
					System.out.println("Unable to access records: " + getLowerBound() + " - " + getUpperBound());
					// and move on.
					stepForward();
					System.out.println("Will retry from " + getLowerBound() + " - " + getUpperBound() + " (+" + getBoundIncrease() + ").");
				} else {
					System.out.println("Stepping back from " + getLowerBound() + "-" + getUpperBound() + " and braking from +" + getBoundIncrease() + ".");
					brake();
					System.out.println("Will retry from " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ").");
				}
			}
		}
		stepForward(timePerRecord);
		return rows;
	}

	private void stepForward(double timePerRecord) {
		setLowerBound(getLowerBound() + getBoundIncrease());
		accelerateIfImproved(timePerRecord);
	}

	private void stepForward() {
		setLowerBound(getLowerBound() + getBoundIncrease());
	}

	private void brake() {
		setBoundIncrease(getBoundIncrease() / 2);
		if (getBoundIncrease() < getMinBoundIncrease()) {
			setBoundIncrease(getMinBoundIncrease());
		}
	}

	private void accelerate() {
		setBoundIncrease(getBoundIncrease() * 2);
		if (getBoundIncrease() > getMaxBoundIncrease()) {
			setBoundIncrease(getMaxBoundIncrease());
		}
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the noMoreRecords
	 */
	private boolean hasMoreRecords() {
		return moreRecords;
	}

	/**
	 * @param noMoreRecords the noMoreRecords to set
	 */
	private void setMoreRecords(boolean noMoreRecords) {
		this.moreRecords = noMoreRecords;
	}

	private void accelerateIfImproved(double timePerRecord) {
		if (timePerRecord < previousTimePerRecord) {
			accelerate();
		} else {
			brake();
		}
		previousTimePerRecord = timePerRecord;
	}

	private int getUpperBound() {
		return getLowerBound() + getBoundIncrease();
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the maxBoundIncrease
	 */
	protected int getMaxBoundIncrease() {
		return maxBoundIncrease;
	}

	/**
	 * Allows the programmer to specify a maximum difference between the lower-
	 * and upper-bounds.
	 *
	 * @param maxBoundIncrease the maxBoundIncrease to set
	 */
	protected void setMaxBoundIncrease(int maxBoundIncrease) {
		this.maxBoundIncrease = maxBoundIncrease;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the MIN_BOUND_INCREASE
	 */
	private int getMinBoundIncrease() {
		return MIN_BOUND_INCREASE;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the boundIncrease
	 */
	protected int getBoundIncrease() {
		return boundIncrease;
	}

	/**
	 * @param boundIncrease the boundIncrease to set
	 */
	private void setBoundIncrease(int boundIncrease) {
		this.boundIncrease = boundIncrease;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the maxBound
	 */
	protected int getMaxBound() {
		return maxBound;
	}

	/**
	 * Allows the programmer to set the last number to be extracted.
	 *
	 * @param maxBound the maxBound to set
	 */
	protected void setMaxBound(int maxBound) {
		this.maxBound = maxBound;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the lowerBound
	 */
	protected int getLowerBound() {
		return lowerBound;
	}

	/**
	 * @param lowerBound the lowerBound to set
	 */
	protected void setLowerBound(int lowerBound) {
		this.lowerBound = lowerBound;
	}

	/**
	 * Changes the default timeout for the underlying query during this
	 * extraction.
	 *
	 * <p>
	 * DBvolution default timeout is 10000 milliseconds to avoid excessive
	 * queries.
	 *
	 * <p>
	 * Use this method to extend or reduce the timeout period as required.
	 *
	 * @param milliseconds
	 */
	protected void setTimeoutInMilliseconds(Integer milliseconds) {
		this.timeoutInMilliseconds = milliseconds;
	}

	private void setQueryTimeout(DBQuery query) {
		if (this.timeoutInMilliseconds == null) {
			query.clearTimeout();
		} else {
			query.setTimeoutInMilliseconds(this.timeoutInMilliseconds);
		}
	}

	/**
	 * Restrict this extraction to only returning the row count.
	 *
	 * <p>
	 * The default is to return the rows found, however it is much more efficient
	 * to only count the rows.
	 *
	 * <p>
	 * Use the method to switch to only counting the rows. Handling the results of
	 * the extraction is still done in {@link #processRows(java.util.List) } but
	 * the list will be empty.
	 * <p>
	 * Use {@link #getRowCount() } to retrieve the row count within {@link #processRows(java.util.List)
	 * }.
	 */
	public void setToCountOnly() {
		countOnly = true;
	}

	/**
	 * Set this extraction to retrieve rows.
	 *
	 * <p>
	 * This is the default behavior.
	 *
	 * <p>
	 * Use this method to switch from {@link #setToCountOnly() } back to
	 * retrieving the full collection of data.
	 *
	 * <p>
	 * Processing of rows extracted is done in {@link #processRows(java.util.List)
	 * }.
	 *
	 *
	 */
	public void setToRetrieveRows() {
		countOnly = false;
	}

	/**
	 * Return the number of rows found by this iteration of the extraction.
	 *
	 * <p>
	 * For use with {@link #setToCountOnly() } but also with {@link #setToRetrieveRows()
	 * }
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the number of rows found in the last partial extraction
	 */
	public Long getRowCount() {
		return rowCount;
	}

}
