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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
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
 * {@link #getQuery(nz.co.gregs.dbvolution.DBDatabase, int, int)} and
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
 * @author Gregory Graham
 */
public abstract class Extractor extends DBScript {
	/*
	 * To change this license header, choose License Headers in Project Properties.
	 * To change this template file, choose Tools | Templates
	 * and open the template in the editor.
	 */

	private int maxBoundIncrease = 10000000;
	private final int minBoundIncrease = 1;
	private int boundIncrease = 10;

	private int maxBound = 200 * 1000000;
	private int startLowerBound = 0;
	private int lowerBound = 0;
	private boolean moreRecords = true;
	private double previousTimePerRecord = Double.MAX_VALUE; // ridiculous default is only to seed the process.
	private Integer timeoutInMilliseconds = 10000;
	public Long rowCount = null;
	private boolean countOnly = false;
	private final DBDatabase database;

	/**
	 * Default constructor.
	 *
	 * @param db
	 */
	public Extractor(DBDatabase db) {
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
	 * @return
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
	 * {@link #getQuery(nz.co.gregs.dbvolution.DBDatabase, int, int)} and
	 * {@link #processRows(java.util.List)} method to provide a dynamic extraction
	 * process that achieves fast results on unreliable or under-resourced
	 * databases.
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
			System.out.println("EXTRACTED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") in "+elapsedTimeInMilliseconds+"ms at " + timePerRecord + "ms/record.");
			double estimatedRequiredTime = timePerRecord * (maxBound - startLowerBound);
			cal.setTime(startTime);
			int secondsValue = (new Double(estimatedRequiredTime / 1000)).intValue();
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
	 * @return an action list
	 * @throws java.io.FileNotFoundException
	 */
	@Override
	public final DBActionList script(DBDatabase db) throws FileNotFoundException, IOException, Exception {
		DBActionList actions = new DBActionList();

		List<DBQueryRow> rows = getRows(db);
		Date startTime = new Date();
		processRows(rows);
		Date finishTime = new Date();
		double timePerRecord = (0.0 + finishTime.getTime() - startTime.getTime()) / getBoundIncrease();
		System.out.println("PROCESSED: " + (getLowerBound() - getBoundIncrease()) + "-" + (getUpperBound() - getBoundIncrease()) + " (+" + getBoundIncrease() + ") at " + timePerRecord + "ms/record.");
		rows = null;

		return actions;
	}

	private List<DBQueryRow> getRows(DBDatabase db) throws AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<DBQueryRow> rows = null;
		this.rowCount=0L;
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
					} else {
						rows = dbQuery.getAllRows();
						rowCount = 0L+ rows.size();
					}
					Date finishTime = new Date();
					final double timeTaken = 0.0 + finishTime.getTime() - startTime.getTime();
					timePerRecord = timeTaken / getBoundIncrease();
					System.out.println("RETRIEVED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") after "+timeTaken+" at " + timePerRecord + "ms/record.");
				}
			} catch (Exception ex) {
				ex.printStackTrace();
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
	 * @return the maxBoundIncrease
	 */
	protected int getMaxBoundIncrease() {
		return maxBoundIncrease;
	}

	/**
	 * Allows the programmer to specify a maximum difference between the lower- and upper-bounds.
	 * 
	 * @param maxBoundIncrease the maxBoundIncrease to set
	 */
	protected void setMaxBoundIncrease(int maxBoundIncrease) {
		this.maxBoundIncrease = maxBoundIncrease;
	}

	/**
	 * @return the minBoundIncrease
	 */
	private int getMinBoundIncrease() {
		return minBoundIncrease;
	}

	/**
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
//
//	protected void setDatabase(DBDatabase database) {
//		this.database = database;
//	}

	protected void setTimeoutInMilliseconds(Integer milliseconds) {
		if (milliseconds != null) {
			this.timeoutInMilliseconds = milliseconds;
		}
	}

	private void setQueryTimeout(DBQuery query) {
		if (this.timeoutInMilliseconds == null) {
			query.clearTimeout();
		} else {
			query.setTimeoutInMilliseconds(this.timeoutInMilliseconds);
		}
	}
	
	public void setToCountOnly(){
		countOnly=true;
	}

	public void setToRetrieveRows(){
		countOnly=false;
	}

}
