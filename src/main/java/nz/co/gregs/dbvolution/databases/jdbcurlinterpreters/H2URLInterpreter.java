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
package nz.co.gregs.dbvolution.databases.jdbcurlinterpreters;

import java.util.HashMap;
import java.util.Map;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.databases.H2DB;

/**
 *
 * @author gregorygraham
 */
public class H2URLInterpreter extends AbstractURLInterpreter {

	private final static HashMap<String, String> DEFAULT_EXTRAS_MAP = new HashMap<>();

	@Override
	public Map<String, String> getDefaultConfigurationExtras() {
		return DEFAULT_EXTRAS_MAP;
	}

	public H2URLInterpreter() {
	}

	@Override
	protected DatabaseConnectionSettings generateSettingsInternal(String jdbcURL, DatabaseConnectionSettings settings) {
		int protocolIndex = 2;
		int restIndex = 4;
		String[] firstSplit = jdbcURL.split(":", restIndex);
		settings.setProtocol(firstSplit[protocolIndex]);
		if (!settings.getProtocol().equals("tcp")
				&& !settings.getProtocol().equals("ssl")
				&& !settings.getProtocol().equals("mem")
				&& !settings.getProtocol().equals("zip")
				&& !settings.getProtocol().equals("file")) {
			settings.setProtocol("");
			restIndex -= 1;
			firstSplit = jdbcURL.split(":", restIndex);
		}
		String restString = firstSplit[restIndex - 1];
//		either
//      //<server>[:<port>]/[<path>]<databaseName>;EXTRA1=THING;EXTRA2=SOMETHING
//      or
//      [<path>]<databaseName>;EXTRA1=THING;EXTRA2=SOMETHING
		if (restString.startsWith("//")) {
			String[] secondSplit = restString.split("/", 4);
			String hostAndPort = secondSplit[2];
			if (hostAndPort.contains(":")) {
				String[] thirdSplit = hostAndPort.split(":");
				settings.setHost(thirdSplit[0]);
				settings.setPort(thirdSplit[1]);
			} else {
				settings.setHost(hostAndPort);
				settings.setPort("9123");
			}
			restString = secondSplit[3];
		}
		// now
		// [<path>]<databaseName>;EXTRA1=THING;EXTRA2=SOMETHING
		String[] fourthSplit = restString.split(";");
		settings.setDatabaseName(fourthSplit[0]);
		settings.setFilename(fourthSplit[0]);
		if (fourthSplit.length > 1) {
			settings.setExtras(DatabaseConnectionSettings.decodeExtras(fourthSplit[1], ";", "=", ";", ""));
		}
		return settings;
	}

	@Override
	protected String generateJDBCURLInternal(DatabaseConnectionSettings settings) {
		final boolean hasNoProtocol = settings.getProtocol()==null||"".equals(settings.getProtocol());
		final String url = "jdbc:h2:"
				+ (hasNoProtocol ? "" : settings.getProtocol() + "://")
				+ (settings.getFilename() == null || settings.getFilename().isEmpty()
				? settings.getDatabaseName()
				: settings.getFilename());
		return url;
	}

	@Override
	public Class<? extends DBDatabase> generatesURLForDatabase() {
		return H2DB.class;
	}

	@Override
	protected DatabaseConnectionSettings setDefaultsInternal(DatabaseConnectionSettings settings) {
		settings.setHost("localhost");
//		settings.setProtocol("file");
		return settings;
	}

	@Override
	public Integer getDefaultPort() {
		return 9123;
	}
}
