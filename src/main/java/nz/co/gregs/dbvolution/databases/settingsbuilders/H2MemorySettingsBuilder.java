/*
 * Copyright 2019 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases.settingsbuilders;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.H2MemoryDBDefinition;
import nz.co.gregs.dbvolution.utility.StringCheck;

public class H2MemorySettingsBuilder
		extends AbstractH2SettingsBuilder<H2MemorySettingsBuilder, H2MemoryDB>
		implements
		UniqueDatabaseCapableSettingsBuilder<H2MemorySettingsBuilder, H2MemoryDB> {

	private static final long serialVersionUID = 1L;

	@Override
	public String getDefaultDriverName() {
		return H2MemoryDB.DRIVER_NAME;
	}

	@Override
	public DBDefinition getDefaultDefinition() {
		return new H2MemoryDBDefinition();
	}

	@Override
	public Class<H2MemoryDB> generatesURLForDatabase() {
		return H2MemoryDB.class;
	}

	@Override
	public String encodeHost(DatabaseConnectionSettings settings) {
		final String encoded
				= StringCheck.check(
						settings.getDatabaseName(),
						settings.getFilename(),
						settings.getInstance()
				);
		return encoded;
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		return "jdbc:h2:mem:";
	}

	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		super.setDefaultsInternal(settings);
		settings.setDatabaseName("unknown");
		settings.setProtocol("mem");
		return settings;
	}

	@Override
	public H2MemoryDB getDBDatabase() throws SQLException {
		return new H2MemoryDB(this);
	}
}
