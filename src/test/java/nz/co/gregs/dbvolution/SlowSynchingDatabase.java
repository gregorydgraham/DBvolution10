/*
 * Copyright 2021 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.Collection;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder;
import nz.co.gregs.dbvolution.utility.Brake;

/**
 *
 * @author gregorygraham
 */
public class SlowSynchingDatabase extends H2MemoryDB {

	private final Brake controller = new Brake();
	private static final long serialVersionUID = 1l;

	private SlowSynchingDatabase(H2MemorySettingsBuilder builder) throws SQLException {
		super(builder);
	}

	/**
	 * Creates a new database with designated label
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @param label the database label to be used internally to identify the
	 * database (not related to the database name)
	 * @return @throws SQLException
	 */
	public static SlowSynchingDatabase createDatabase(String label) throws SQLException {
		return new SlowSynchingDatabase(new H2MemorySettingsBuilder().setLabel(label));
	}

	/**
	 * Creates a new database with random (UUID based) name and label.
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @return @throws SQLException
	 */
	public static SlowSynchingDatabase createANewRandomDatabase() throws SQLException {
		return createANewRandomDatabase("", "");
	}

	/**
	 * Creates a new database with random (UUID based name).
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @param prefix the string to add before the database name and label
	 * @param postfix the string to add after the database name and label
	 * @return @throws SQLException
	 */
	public static SlowSynchingDatabase createANewRandomDatabase(String prefix, String postfix) throws SQLException {
		final H2MemorySettingsBuilder settings = new H2MemorySettingsBuilder().withUniqueDatabaseName();
		settings.setDatabaseName(prefix + settings.getDatabaseName() + postfix);
		settings.setLabel(settings.getDatabaseName());
		return new SlowSynchingDatabase(settings);
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return SlowSynchingDBTable.getInstance(this, example, controller);
	}

	public Brake getBrake() {
		return controller;
	}

	private static class SlowSynchingDBTable<E extends DBRow> extends DBTable<E> {

		private Brake brake;

		private SlowSynchingDBTable(DBDatabase database, E exampleRow) {
			super(database, exampleRow);
		}

		static <R extends DBRow> DBTable<R> getInstance(DBDatabase database, R example, Brake brake) {
			SlowSynchingDBTable<R> dbTable = new SlowSynchingDBTable<>(database, example);
			dbTable.brake = brake;
			return dbTable;
		}

		@Override
		public DBActionList insert(Collection<E> newRows) throws SQLException {
			brake.checkBrake();
			return super.insert(newRows);
		}
	}
}
