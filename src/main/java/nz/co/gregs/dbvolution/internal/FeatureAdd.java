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
package nz.co.gregs.dbvolution.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.exceptions.ExceptionDuringDatabaseFeatureSetup;

/**
 *
 * @author gregorygraham
 */
public interface FeatureAdd {

	public String[] createSQL();

	public String name();

	public default String[] optionalPreparationSQL() {
		return new String[]{};
	}
	
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
			justification = "The strings are actually constant but made dynamically")
	public default void add(Statement stmt) throws ExceptionDuringDatabaseFeatureSetup {
		try {
			final String[] optionalSQL = optionalPreparationSQL();
			if (optionalSQL != null && optionalSQL.length > 0) {
				for (String sql : optionalSQL) {
					try {
						stmt.execute(sql);
					} catch (Exception ex) {
						throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + featureName(), ex);
					}
				}
			}
		} catch (ExceptionDuringDatabaseFeatureSetup exc) {
			;// Optional SQL may fail and we'll continue
		}
		try {
			final String[] optionalSQL = dropSQL();
			if (optionalSQL != null && optionalSQL.length > 0) {
				for (String sql : optionalSQL) {
					try {
						stmt.execute(sql);
					} catch (Exception ex) {
						throw new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + featureName(), ex);
					}
				}
			}
		} catch (ExceptionDuringDatabaseFeatureSetup exc) {
			;// Optional SQL may fail and we'll continue
		}
		final String[] requiredSQL = createSQL();
		if (requiredSQL != null && requiredSQL.length > 0) {
			for (String sql : requiredSQL) {
				try {
					stmt.execute(sql);
				} catch (Exception ex) {
					final ExceptionDuringDatabaseFeatureSetup setupException = new ExceptionDuringDatabaseFeatureSetup("FAILED TO ADD FEATURE: " + featureName(), ex);
					LOGGER.log(Level.SEVERE, "" + setupException.getMessage());
					throw setupException;
				}
			}
		}
	}
	static final Logger LOGGER = Logger.getLogger(FeatureAdd.class.getName());

	public default String featureName() {
		return name();
	}

	public default String[] dropSQL() {
		return new String[]{};
	}
}
