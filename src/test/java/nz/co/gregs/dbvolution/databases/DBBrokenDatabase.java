/*
 * Copyright 2024 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.actions.BrokenAction;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.actions.DBQueryable;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;

/**
 *
 * @author gregorygraham
 */
public class DBBrokenDatabase extends DBDatabaseHandle {
	
	private static final long serialVersionUID = 1L;
	public boolean useBrokenAction = false;
	public boolean useBrokenQuery = false;
	private final transient Object ACTION = new Object();

	public DBBrokenDatabase(DBDatabase db) throws SQLException {
		super(db);
	}

	@Override
	public DBQueryable executeDBQuery(DBQueryable query) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException, NoAvailableDatabaseException {
		if (useBrokenQuery) {
			System.out.println("DATABASE " + getLabel() + " USING BROKEN QUERY");
			throw new SQLException("BROKEN QUERY");
		} else {
			return super.executeDBQuery(query);
		}
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		if (useBrokenAction) {
			try {
				System.out.println("DATABASE " + getLabel() + " INSERTING  BROKEN ACTION");
				final DBActionList result = super.executeDBAction(new BrokenAction());
				return result;
			} finally {
				synchronized (ACTION) {
					ACTION.notifyAll();
				}
			}
		} else {
			return super.executeDBAction(action);
		}
	}
	
}
