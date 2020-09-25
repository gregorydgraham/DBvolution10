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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.settingsbuilders.PostgresSettingsBuilder;
import org.testcontainers.containers.PostgisContainerProvider;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 *
 * @author gregorygraham
 */
public class PostgresContainerDB extends PostgresDB{

	private static final long serialVersionUID = 1l;
	protected final PostgreSQLContainer<?> storedContainer;

	public static PostgresContainerDB getInstance() {
		return getLabelledInstance("Unlabelled");
	}

	public static PostgresContainerDB getLabelledInstance(String label) {
		PostgreSQLContainer<?> container = (PostgreSQLContainer) new PostgisContainerProvider().newInstance();
		ContainerUtils.startContainer(container);
		try {
			PostgresContainerDB dbdatabase = new PostgresContainerDB(container, label);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(PostgresContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create Postgres Database in Docker Container", ex);
		}
	}

	public PostgresContainerDB(PostgreSQLContainer<?> container, String label) throws SQLException {
		this(
				container, 
				ContainerUtils.getContainerSettings(new PostgresSettingsBuilder(), container, label)
		);
	}

	public PostgresContainerDB(PostgreSQLContainer<?> container, PostgresSettingsBuilder settings) throws SQLException {
		super(settings);
		this.storedContainer = container;
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

}
