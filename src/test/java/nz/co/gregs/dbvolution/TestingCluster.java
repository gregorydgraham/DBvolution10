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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.actions.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.settingsbuilders.DBDatabaseClusterSettingsBuilder;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.utility.Brake;

/**
 *
 * @author gregorygraham
 */
public class TestingCluster extends DBDatabaseCluster {

	private static final long serialVersionUID = 1l;

	private final Brake controller = new Brake();

	private boolean failOnInsert = false;
	private boolean failOnUpdate = false;
	private boolean failOnCreateTable = false;
	private boolean failOnDelete = false;

	private TestingCluster(DBDatabaseClusterSettingsBuilder config, DBDatabase... databases) throws SQLException {
		super(config, databases);
	}

	public void setFailOnUpdate(boolean failOnUpdate) {
		this.failOnUpdate = failOnUpdate;
	}

	private TestingCluster(DBDatabaseClusterSettingsBuilder builder) throws SQLException {
		super(builder);
	}

	/**
	 * Creates a new database with designated name and label.
	 *
	 * <p>
	 * Great for we you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @param label the database label to be used internally to identify the
	 * database (not related to the database name)
	 * @return a database that takes a long time to synch
	 * @throws SQLException if a database error occurs
	 */
	public static TestingCluster createDatabase(String label) throws SQLException {
		return new TestingCluster(new DBDatabaseClusterSettingsBuilder().setLabel(label));
	}

	/**
	 * Creates a new database with random (UUID based) name and label.
	 *
	 * <p>
	 * Great for when you just need to make a database and don't need to keep
	 * it.</p>
	 *
	 * @return a new slow synching database
	 * @throws SQLException if a database error occurs
	 */
	public static TestingCluster createANewRandomDatabase() throws SQLException {
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
	 * @return a new slow synching database
	 * @throws SQLException if a database error occurs
	 */
	public static TestingCluster createANewRandomDatabase(String prefix, String postfix) throws SQLException {
		final DBDatabaseClusterSettingsBuilder settings = new DBDatabaseClusterSettingsBuilder().withUniqueDatabaseName();
		settings.setDatabaseName(prefix + settings.getDatabaseName() + postfix);
		settings.setLabel(settings.getDatabaseName());
		return new TestingCluster(settings);
	}

	@Override
	public <R extends DBRow> DBTable<R> getDBTable(R example) {
		return SlowSynchingDBTable.getInstance(this, example, controller);
	}

	public Brake getBrake() {
		return controller;
	}

	public void setFailOnInsert(boolean failOnInsert) {
		this.failOnInsert = failOnInsert;
	}

	public void setFailOnCreateTable(boolean failOnCreateTable) {
		this.failOnCreateTable = failOnCreateTable;
	}

	public void setFailOnDelete(boolean failOnDelete) {
		this.failOnDelete = failOnDelete;
	}

	@Override
	public DBActionList executeDBAction(DBAction action) throws SQLException, NoAvailableDatabaseException {
		if (failOnInsert && (action instanceof DBInsert)) {
			throw new SQLException("DELIBERATELY FAILING DURING INSERT");
		}
		if (failOnUpdate && (action instanceof DBUpdate)) {
			throw new SQLException("DELIBERATELY FAILING DURING UPDATE");
		}
		if (failOnCreateTable && (action instanceof DBCreateTable)) {
			throw new SQLException("DELIBERATELY FAILING DURING CREATE TABLE");
		}
		if (failOnDelete && (action instanceof DBDelete)) {
			throw new SQLException("DELIBERATELY FAILING DURING DELETE");
		}
		return super.executeDBAction(action);
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
		public DBActionList insert(E newRow) throws SQLException {
			brake.checkBrake();
			return super.insert(newRow);
		}

		@Override
		public DBActionList update(E newRow) throws SQLException {
			brake.checkBrake();
			return super.update(newRow);
		}

		@Override
		protected DBActionList updateAnyway(E row) throws SQLException {
			brake.checkBrake();
			return super.updateAnyway(row);
		}

		@Override
		public DBActionList delete(Collection<E> oldRows) throws SQLException {
			brake.checkBrake();
			return super.delete(oldRows);
		}
	}

	/**
	 * Creates a new cluster without auto-start, auto-rebuild, auto-connect, nor
	 * auto-reconnect.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param databases a database to build the cluster with
	 * @return a cluster with a random name based on the manual configuration and
	 * the database
	 * @throws SQLException database errors may be thrown during initialisation
	 */
	public static TestingCluster randomManualCluster(DBDatabase... databases) throws SQLException {
		final String dbName = DBDatabaseCluster.getRandomClusterName();
		return manualCluster(dbName, databases);
	}

	/**
	 * Creates a new cluster without auto-start, auto-rebuild, auto-connect, nor
	 * auto-reconnect.
	 *
	 * <p>
	 * Use this method to ensure that the new cluster will not clash with any
	 * existing clusters.</p>
	 *
	 * @param dbName the name for the cluster 
	 * @param databases a database to build the cluster with
	 * @return a cluster based on the manual configuration and the database
	 * @throws SQLException database errors may be thrown during initialisation
	 */
	public static TestingCluster manualCluster(String dbName, DBDatabase... databases) throws SQLException {
		DBDatabaseClusterSettingsBuilder config = new DBDatabaseClusterSettingsBuilder().setLabel(dbName).setConfiguration(Configuration.fullyManual());
		List<DBDatabase> dbs = Arrays.asList(databases);
		List<DatabaseConnectionSettings> hosts = dbs.stream().map(d -> d.getSettings()).collect(Collectors.toList());
		
		config.setClusterHosts(hosts);
		TestingCluster cluster = new TestingCluster(config, databases);
		return cluster;
	}
}
