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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 *
 * @author gregorygraham
 */
public class Postgres10ContainerDB extends PostgresDB {

	private static final long serialVersionUID = 1l;

	protected final PostgreSQLContainer<?> storedContainer;

	protected static final String DEFAULT_LABEL = "Unlabelled";
	protected static final String DEFAULT_TAG = "10-3.0-alpine";
	protected static final String DEFAULT_CONTAINER = "postgis/postgis";

	public static Postgres10ContainerDB getInstance() {
		return getLabelledInstance(DEFAULT_LABEL);
	}

	public static PostgresDB getInstance(String image, String tag) {
		return getLabelledInstance(DEFAULT_LABEL, image, tag);
	}

	public static Postgres10ContainerDB getLabelledInstance(String unlabelled) {
		return getLabelledInstance(unlabelled, DEFAULT_CONTAINER, DEFAULT_TAG);
	}

	public static Postgres10ContainerDB getLabelledInstance(String label, String containerRepo, String tag) {
		DockerImageName imageName = DockerImageName.parse(containerRepo + ":" + tag).asCompatibleSubstituteFor("postgres");
		PostgreSQLContainer<?> container = new PostgreSQLContainer<>(imageName);
//		PostgreSQLContainer<?> container = (PostgreSQLContainer) new PostgisContainerProvider().newInstance("10");
		ContainerUtils.startContainer(container);
		try {
			Postgres10ContainerDB dbdatabase = new Postgres10ContainerDB(container, label);
			return dbdatabase;
		} catch (SQLException ex) {
			Logger.getLogger(Postgres10ContainerDB.class.getName()).log(Level.SEVERE, null, ex);
			throw new RuntimeException("Unable To Create Postgres Database in Docker Container", ex);
		}
	}

	public Postgres10ContainerDB(PostgreSQLContainer<?> container, String label) throws SQLException {
		this(
				container,
				ContainerUtils.getContainerSettings(new PostgresSettingsBuilder(), container, label)
		);
	}

	public Postgres10ContainerDB(PostgreSQLContainer<?> container, PostgresSettingsBuilder settings) throws SQLException {
		super(settings);
		this.storedContainer = container;
	}

	@Override
	public synchronized void stop() {
		super.stop();
		storedContainer.stop();
	}

}
