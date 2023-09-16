/*
 * Copyright 2023 Gregory Graham.
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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.utility.Timer;

/**
 *
 * @author gregorygraham
 */
public class NoOpDBAction extends DBAction {

	private static int counter = 0;
	private static final long serialVersionUID = 1L;
	private final long WAIT_TIME_IN_MILLIS;

	public NoOpDBAction() {
		super(null, QueryIntention.NO_OP);
		WAIT_TIME_IN_MILLIS = 0;
	}

	public NoOpDBAction(long waitTimeInMilliseconds) {
		super(null, QueryIntention.NO_OP);
		if (waitTimeInMilliseconds > 0) {
			WAIT_TIME_IN_MILLIS = waitTimeInMilliseconds;
		} else {
			WAIT_TIME_IN_MILLIS = 0;
		}
	}

	@Override
	public boolean hasRun() {
		return false;
	}

	@Override
	public boolean hasSucceeded() {
		return false;
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		return new DBActionList();
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		return new ArrayList<>(0);
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		if (WAIT_TIME_IN_MILLIS > 0) {
			Timer timer = Timer.timer();
			try {
				timer = Timer.timer();
				Thread.sleep(WAIT_TIME_IN_MILLIS);
//				System.out.println("STALLED "+this);
				System.out.println("STALLED "+this+" FOR: " + timer.duration());
			} catch (InterruptedException ex) {
				timer.stop();
				Logger.getLogger(NoOpDBAction.class.getName()).log(Level.SEVERE, null, ex);
				
				System.out.println("INTERUPTED AFTER: " + timer.duration());
			}
		}
		return new DBActionList();
	}

	private synchronized String incCounter() {
		counter++;
		return "" + counter;
	}
}
