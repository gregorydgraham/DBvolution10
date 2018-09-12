/*
 * Copyright 2018 gregorygraham.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates a DBDatabaseCluster based on the information in yamlConfigFilename.
 *
 * <p>
 * Searches the application's file system from "." looking for a file named
 * yamlConfigFilename and uses the details within to create a
 * DBDatabaseCluster.</p>
 *
 * <p>
 * the file can be placed anywhere in the file system below "." but must be
 * named exactly at specified in the constructor. Only the first such file found
 * will be used and the search order is not defined so avoid creating multiple
 * versions of yamlConfigFilename.</p>
 *
 * <p>
 * The file's contents must be YAML for an array of
 * {@link DatabaseConnectionSettings} objects. Each DatabaseConnectionSettings
 * object has a DBDatabase's canonical class name, username and password, and
 * either a JDBC URL or a combination of host, port, instance, database, schema,
 * and extras specifying the configuration of the DBDatabase. The DBDatabase
 * constructor that takes a DatabaseConnectionSettings (and only that) will be
 * called and the resulting DBDatabase instance will be added to the
 * cluster.</p>
 * <p>
 * Example yamlConfigFilename contents:</p>
 * <pre>
 * ---
 * - dbdatabase: "nz.co.gregs.dbvolution.databases.H2MemoryDB"
 *   url: "jdbc:h2:mem:TestDatabase.h2"
 *   username: "admin"
 *   password: "admin"
 * - dbdatabase: "nz.co.gregs.dbvolution.databases.MySQLDB"
 *   username: "admin"
 *   host: "myserver.com"
 *   port: "40006"
 *   instance: "myinstance"
 *   database: "appdatabase"
 *   schema: "default"</pre>
 *
 * <p>
 * DBDatabase classes without a url/username/password based constructor cannot
 * be created. Note that this means you cannot add a DBDatabaseCluster to this
 * cluster via this method.</p>
 *
 * @author gregorygraham
 */
public class DBDatabaseClusterWithConfigFile extends DBDatabaseCluster {

	static final long serialVersionUID = 1L;

	private final String yamlConfigFilename;

	/**
	 * Creates a DBDatabaseCluster based on the information in yamlConfigFilename.
	 *
	 * <p>
	 * Searches the application's file system from "." looking for a file named
	 * yamlConfigFilename and uses the details within to create a
	 * DBDatabaseCluster.</p>
	 *
	 * <p>
	 * the file can be placed anywhere in the file system below "." but must be
	 * named exactly at specified in the constructor. Only the first such file
	 * found will be used and the search order is not defined so avoid creating
	 * multiple versions of yamlConfigFilename.</p>
	 *
	 * <<p>
	 * The file's contents must be YAML for an array of
	 * {@link DatabaseConnectionSettings} objects. Each DatabaseConnectionSettings
	 * object has a DBDatabase's canonical class name, username and password, and
	 * either a JDBC URL or a combination of host, port, instance, database,
	 * schema, and extras specifying the configuration of the DBDatabase. The
	 * DBDatabase constructor that takes a DatabaseConnectionSettings (and only
	 * that) will be called and the resulting DBDatabase instance will be added to
	 * the cluster.</p>
	 * <p>
	 * Example yamlConfigFilename contents:</p>
	 * <pre>
	 * ---
	 * - dbdatabase: "nz.co.gregs.dbvolution.databases.H2MemoryDB"
	 *   url: "jdbc:h2:mem:TestDatabase.h2"
	 *   username: "admin"
	 *   password: "admin"
	 * - dbdatabase: "nz.co.gregs.dbvolution.databases.MySQLDB"
	 *   username: "admin"
	 *   host: "myserver.com"
	 *   port: "40006"
	 *   instance: "myinstance"
	 *   database: "appdatabase"
	 *   schema: "default"</pre>
	 *
	 * <p>
	 * DBDatabase classes without a url/username/password based constructor cannot
	 * be created. Note that this means you cannot add a DBDatabaseCluster to this
	 * cluster via this method.</p>
	 *
	 * @author gregorygraham
	 * @param yamlConfigFilename
	 * @throws DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound
	 * @throws DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster
	 */
	public DBDatabaseClusterWithConfigFile(String yamlConfigFilename) throws NoDatabaseConfigurationFound, UnableToCreateDatabaseCluster {
		super();
		this.yamlConfigFilename = yamlConfigFilename;
		findDatabaseConfigurationAndApply(yamlConfigFilename);
	}

	/**
	 * Creates a DBDatabaseCluster based on the information in yamlConfigFile.
	 *
	 * <p>
	 * Uses the details within the file specified to create a
	 * DBDatabaseCluster.</p>
	 *
	 * <p>
	 * The file's contents must be YAML for an array of
	 * {@link DatabaseConnectionSettings} objects. Each DatabaseConnectionSettings
	 * object has a DBDatabase's canonical class name, username and password, and
	 * either a JDBC URL or a combination of host, port, instance, database,
	 * schema, and extras specifying the configuration of the DBDatabase. The
	 * DBDatabase constructor that takes a DatabaseConnectionSettings (and only
	 * that) will be called and the resulting DBDatabase instance will be added to
	 * the cluster.</p>
	 * <p>
	 * Example yamlConfigFile contents:</p>
	 * <pre>
	 * ---
	 * - dbdatabase: "nz.co.gregs.dbvolution.databases.H2MemoryDB"
	 *   url: "jdbc:h2:mem:TestDatabase.h2"
	 *   username: "admin"
	 *   password: "admin"
	 * - dbdatabase: "nz.co.gregs.dbvolution.databases.MySQLDB"
	 *   username: "admin"
	 *   host: "myserver.com"
	 *   port: "40006"
	 *   instance: "myinstance"
	 *   database: "appdatabase"
	 *   schema: "default"</pre>
	 *
	 * <p>
	 * DBDatabase classes without a url/username/password based constructor cannot
	 * be created. Note that this means you cannot add a DBDatabaseCluster to this
	 * cluster via this method.</p>
	 *
	 * @author gregorygraham
	 * @param yamlConfigFile
	 * @throws DBDatabaseClusterWithConfigFile.NoDatabaseConfigurationFound
	 * @throws DBDatabaseClusterWithConfigFile.UnableToCreateDatabaseCluster
	 */
	public DBDatabaseClusterWithConfigFile(File yamlConfigFile) throws NoDatabaseConfigurationFound, UnableToCreateDatabaseCluster {
		super();
		this.yamlConfigFilename = yamlConfigFile.getName();
		parseYAMLAndAddDatabases(yamlConfigFile, yamlConfigFilename);
	}

	public void reloadConfiguration() throws NoDatabaseConfigurationFound, UnableToCreateDatabaseCluster {
		this.removeDatabases(details.getAllDatabases());
		findDatabaseConfigurationAndApply(yamlConfigFilename);
	}

	private void findDatabaseConfigurationAndApply(String yamlConfigFilename) throws NoDatabaseConfigurationFound, UnableToCreateDatabaseCluster {
		try {
			final DefaultConfigFinder finder = new DefaultConfigFinder(yamlConfigFilename);
			Files.walkFileTree(Paths.get("."), finder);
			if (finder.configPath != null) {
				Path filePath = finder.configPath;
				File file = filePath.toFile();

				parseYAMLAndAddDatabases(file, yamlConfigFilename);
				LOG.info("Completed Database");
			} else {
				throw new NoDatabaseConfigurationFound(yamlConfigFilename);
			}
		} catch (IOException ex) {
			Logger.getLogger(DBDatabaseClusterWithConfigFile.class.getName()).log(Level.SEVERE, null, ex);
			throw new UnableToCreateDatabaseCluster(ex);
		}
	}

	private void parseYAMLAndAddDatabases(File file, String yamlConfigFilename) throws UnableToCreateDatabaseCluster {
		try {
			final YAMLFactory yamlFactory = new YAMLFactory();
			YAMLParser parser = yamlFactory.createParser(file);
			ObjectMapper mapper = new ObjectMapper(yamlFactory);
			DatabaseConnectionSettings[] settingsArray = mapper.readValue(parser, DatabaseConnectionSettings[].class);
			if (settingsArray.length == 0) {
				throw new NoDatabaseConfigurationFound(yamlConfigFilename);
			} else {
				for (DatabaseConnectionSettings settings : settingsArray) {

					DBDatabase database = settings.createDBDatabase();

					if (database != null) {
						LOG.info("Adding Database: " + settings.getDbdatabase() + ":" + database.getUrlFromSettings(settings) + ":" + settings.getUsername());
						this.addDatabaseAndWait(database);
					}
				}
			}
		} catch (IOException | NoDatabaseConfigurationFound | ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SQLException ex) {
			Logger.getLogger(DBDatabaseClusterWithConfigFile.class.getName()).log(Level.SEVERE, null, ex);
			throw new UnableToCreateDatabaseCluster(ex, file);
		}
	}

	public static class DefaultConfigFinder
			extends SimpleFileVisitor<Path> {

		private int filesChecked = 0;
		private final String yamlConfigFilename;
		private Path configPath;
		private final List<String> visitedFiles = new ArrayList<>();

		DefaultConfigFinder(String yamlConfigFilename) {
			this.yamlConfigFilename = yamlConfigFilename;
		}

		// Compares the glob pattern against
		// the file or directory name.
		FileVisitResult find(Path path) {
			if (filesChecked > 100000) {
				return FileVisitResult.TERMINATE;
			}
			filesChecked++;
			if (!visited(path)) {
				Path name = path.getFileName();
				if (name != null && name.toString().equals(yamlConfigFilename)) {
					configPath = path;
					return FileVisitResult.TERMINATE;
				}
			}
			return FileVisitResult.CONTINUE;
		}

		// Invoke the pattern matching
		// method on each file.
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
			return find(file);

		}

		// Invoke the pattern matching
		// method on each directory.
		@Override
		public FileVisitResult preVisitDirectory(Path dir,
				BasicFileAttributes attrs) {
			return find(dir);
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			if (configPath == null) {
				LOG.debug("Unable To Find Database Cluster Config File In: " + dir.toAbsolutePath().toString());
			}
			return super.postVisitDirectory(dir, exc);
		}

		private boolean visited(Path path) {
			String key = path.toAbsolutePath().toString();
			if (visitedFiles.contains(key)) {
				return true;
			} else {
				visitedFiles.add(key);
			}
			return false;
		}
	}

	public static class NoDatabaseConfigurationFound extends Exception {

		static final long serialVersionUID = 1L;

		private NoDatabaseConfigurationFound(String yamlConfigFilename) {
			super("No DBDatabase Configuration File named \"" + yamlConfigFilename + "\" was found in the filesystem: check the filename and ensure that the location is accessible from \".\"" + (Paths.get(".").toAbsolutePath()));
		}
	}

	public static class NoDatabasesSpecifiedWithinConfiguration extends Exception {

		static final long serialVersionUID = 1L;

		private NoDatabasesSpecifiedWithinConfiguration(String yamlConfigFilename) {
			super("The Configuration File named \"" + yamlConfigFilename + "\" was found but no databases were specified within: check the filename, location, and syntax of databases within .");
		}
	}

	public static class UnableToCreateDatabaseCluster extends Exception {

		static final long serialVersionUID = 1L;

		public UnableToCreateDatabaseCluster(Exception ex) {
			super("Unable Create DBDatabaseCluster Due To Exception", ex);
		}

		private UnableToCreateDatabaseCluster(Exception ex, File file) {
			super("Unable Create DBDatabaseCluster Due To Exception: "+file.getAbsolutePath(), ex);
		}
	}
}
