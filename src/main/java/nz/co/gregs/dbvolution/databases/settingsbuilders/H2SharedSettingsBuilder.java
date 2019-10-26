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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.H2SharedDB;
import nz.co.gregs.dbvolution.utility.DefaultString;

public class H2SharedSettingsBuilder extends AbstractH2SettingsBuilder<H2SharedSettingsBuilder>
		implements RemoteCapableSettingsBuilder<H2SharedSettingsBuilder>,
		ProtocolCapableSettingsBuilder<H2SharedSettingsBuilder> {

//	@Override
//	public String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
//		String hostname = defaultString(settings.getHost(), "localhost");
//		String port = defaultString(settings.getPort(), "" + getDefaultPort());
//		String protocol = defaultString(settings.getProtocol(), "tcp");
//		return "jdbc:h2:" + protocol + "://" + hostname + ":" + port + "/" + settings.getDatabaseName();
//	}
	@Override
	protected String encodeHost(DatabaseConnectionSettings settings) {
		String hostname = DefaultString.check(settings.getHost(), "localhost");
		String port = DefaultString.check(settings.getPort(), "" + getDefaultPort());
		final String databaseName = DefaultString.check(settings.getDatabaseName(), settings.getFilename(), settings.getInstance());
		return hostname + ":" + port + "/" + databaseName;
	}

	@Override
	protected String getJDBCURLPreamble(DatabaseConnectionSettings settings) {
		String protocol = defaultString(settings.getProtocol(), "tcp");
		return "jdbc:h2:" + protocol + "://";
	}

	@Override
	public DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		super.setDefaultsInternal(settings);
		settings.setProtocol("tcp");
		return settings;
	}

	@Override
	public Class<? extends DBDatabase> generatesURLForDatabase() {
		return H2SharedDB.class;
	}

	private String defaultString(String initialValue, String defaultValue) {
		return initialValue == null || initialValue.isEmpty() ? defaultValue : initialValue;
	}
}
