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
 *
 * @author gregory.graham
 */
public abstract class Extractor  extends DBScript {
	/*
	 * To change this license header, choose License Headers in Project Properties.
	 * To change this template file, choose Tools | Templates
	 * and open the template in the editor.
	 */

	private int maxBoundIncrease = 10000000;
	private int minBoundIncrease = 1;
	private int boundIncrease = 10;

	private int maxBound = 200 * 1000000;
	private int startLowerBound = 0;
	private int lowerBound = 0;
	boolean foundSomeVehicles = false;
	private boolean moreRecords = true;
	private double previousTimePerRecord = Double.MAX_VALUE; // ridiculous default is only to seed the process.
	private DBDatabase database;

//	private final boolean printedHeader = false;
//	private final File csvFile = null; 
//	private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
//	BufferedOutputStream csvOutput;
//	{
//		FileOutputStream fileOutputStream;
//		try {
//			fileOutputStream = new FileOutputStream(getCsvFile());
//			csvOutput = new BufferedOutputStream(fileOutputStream);
//		} catch (FileNotFoundException ex) {
//			throw new RuntimeException(ex);
//		}
//	}
	
	public Extractor(DBDatabase db){
		database = db;
	}

	abstract public void processRows(List<DBQueryRow> rows) throws Exception ;

	abstract public DBQuery getQuery(DBDatabase db, int lowerbound, int upperbound);



	private DBDatabase getDatabase() {
		return database;
	}

	public final DBActionList extract() throws Exception {
		DBActionList actions = new DBActionList();
//		setCsvFile();
		DBDatabase db = getDatabase();
		startLowerBound = lowerBound;
		Date startTime = new Date();
		GregorianCalendar cal = new GregorianCalendar();
		while (hasMoreRecords()) {
			actions.addAll(db.test(this));
			Date finishTime = new Date();
			final double elapsedTimeInMilliseconds = 0.0 + finishTime.getTime() - startTime.getTime();
			double timePerRecord = elapsedTimeInMilliseconds / (lowerBound - startLowerBound);
			System.out.println("EXTRACTED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") at " + timePerRecord + "ms/record.");
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

	protected List<DBQueryRow> getRows(DBDatabase db) throws AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<DBQueryRow> rows = null;
		double timePerRecord = 10000.0;
		while (hasMoreRecords() && rows == null) {
			try {
				if (getLowerBound() > getMaxBound()) {
					setMoreRecords(false);
				} else {
					System.out.println("RETRIEVING: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ")");
					DBQuery dbQuery = getQuery(db, getLowerBound(), getUpperBound());
					Date startTime = new Date();
					rows = dbQuery.getAllRows();
					Date finishTime = new Date();
					timePerRecord = (0.0 + finishTime.getTime() - startTime.getTime()) / getBoundIncrease();
					System.out.println("RETRIEVED: " + getLowerBound() + "-" + getUpperBound() + " (+" + getBoundIncrease() + ") at " + timePerRecord + "ms/record.");
				}
			} catch (SQLException ex) {
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

	protected void stepForward(double timePerRecord) {
		setLowerBound(getLowerBound() + getBoundIncrease());
		accelerateIfImproved(timePerRecord);
	}

	protected void stepForward() {
		setLowerBound(getLowerBound() + getBoundIncrease());
	}

	protected void brake() {
		setBoundIncrease(getBoundIncrease() / 2);
		if (getBoundIncrease() < getMinBoundIncrease()) {
			setBoundIncrease(getMinBoundIncrease());
		}
	}

	protected void accelerate() {
		setBoundIncrease(getBoundIncrease() * 2);
		if (getBoundIncrease() > getMaxBoundIncrease()) {
			setBoundIncrease(getMaxBoundIncrease());
		}
	}

	/**
	 * @return the noMoreRecords
	 */
	protected boolean hasMoreRecords() {
		return moreRecords;
	}

	/**
	 * @param noMoreRecords the noMoreRecords to set
	 */
	protected void setMoreRecords(boolean noMoreRecords) {
		this.moreRecords = noMoreRecords;
	}

	/**
	 * @return the csvFile
	 */
//	protected File getCsvFile() {
//		if (csvFile == null) {
//			this.setCsvFile();
//		}
//		return csvFile;
//	}

	/**
	 */
//	protected void setCsvFile() {
//		this.csvFile = new File("VIS_" + extractorFileDesignation() + "_" + (new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())) + ".csv");
//	}

	protected void accelerateIfImproved(double timePerRecord) {
		if (timePerRecord < previousTimePerRecord) {
			accelerate();
		} else {
			brake();
		}
		previousTimePerRecord = timePerRecord;
	}

	protected int getUpperBound() {
		return getLowerBound() + getBoundIncrease();
	}

	/**
	 * @return the maxBoundIncrease
	 */
	protected int getMaxBoundIncrease() {
		return maxBoundIncrease;
	}

	/**
	 * @param maxBoundIncrease the maxBoundIncrease to set
	 */
	protected void setMaxBoundIncrease(int maxBoundIncrease) {
		this.maxBoundIncrease = maxBoundIncrease;
	}

	/**
	 * @return the minBoundIncrease
	 */
	protected int getMinBoundIncrease() {
		return minBoundIncrease;
	}

	/**
	 * @param minBoundIncrease the minBoundIncrease to set
	 */
	protected void setMinBoundIncrease(int minBoundIncrease) {
		this.minBoundIncrease = minBoundIncrease;
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
	protected void setBoundIncrease(int boundIncrease) {
		this.boundIncrease = boundIncrease;
	}

	/**
	 * @return the maxBound
	 */
	protected int getMaxBound() {
		return maxBound;
	}

	/**
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

	protected void setDatabase(DBDatabase database) {
		this.database = database;
	}
	
}
