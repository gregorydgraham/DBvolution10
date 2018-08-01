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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
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
 * named exactly yamlConfigFilename. Only the first such file found will be used
 * and the search order is not defined so avoid creating multiple versions of
 * yamlConfigFilename.</p>
 *
 * <p>
 * The file's contents must be YAML for an array of
 * DBDatabaseClusterWithConfigFile.DBDataSource objects. Each DBDataSource
 * object has a DBDatabase's canonical class name and a string for url,
 * username, and password specifying the configuration of that DBDatabase. The
 * constructor of the DBDatabase class that takes 3 strings (and only that) will
 * be called and the resulting DBDatabase instance will be added to the
 * cluster.</p>
 * <p>
 * Example yamlConfigFilename contents:</p>
 * <pre>
 * ---
 * - dbDatabase: "nz.co.gregs.dbvolution.databases.H2MemoryDB"
 *   url: "jdbc:h2:mem:TestDatabase.h2"
 *   username: "admin"
 *   password: "admin"
 * - dbDatabase: "nz.co.gregs.dbvolution.databases.SQLiteDB"
 *   url: "jdbc:sqlite:TestDatabase.sqlite"
 *   username: "admin"
 *   password: "admin"</pre>
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

	public DBDatabaseClusterWithConfigFile(String yamlConfigFilename) throws NoDatabaseConfigurationFound, UnableToCreateDatabaseCluster {
		super();
		this.yamlConfigFilename = yamlConfigFilename;
		findDatabaseConfigurationAndApply(yamlConfigFilename);
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

				final YAMLFactory yamlFactory = new YAMLFactory();
				YAMLParser parser = yamlFactory.createParser(file);
				ObjectMapper mapper = new ObjectMapper(yamlFactory);
				DBDataSource[] dbs = mapper.readValue(parser, DBDataSource[].class);

				if (dbs.length == 0) {
					throw new NoDatabaseConfigurationFound(yamlConfigFilename);
				} else {
					for (DBDataSource db : dbs) {

						DBDatabase database = db.createDBDatabase();

						if (database != null) {
							LOG.info("Adding Database: " + db.dbDatabase + ":" + db.url + ":" + db.username);
							this.addDatabaseAndWait(database);
						}
					}
				}
				LOG.info("Completed Database");
			}
		} catch (IOException | ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SQLException ex) {
			Logger.getLogger(DBDatabaseClusterWithConfigFile.class.getName()).log(Level.SEVERE, null, ex);
			throw new UnableToCreateDatabaseCluster(ex);
		}
	}

	public static class DefaultConfigFinder
			extends SimpleFileVisitor<Path> {

		private final String yamlConfigFilename;
		private Path configPath;

		DefaultConfigFinder(String yamlConfigFilename) {
			this.yamlConfigFilename = yamlConfigFilename;
		}

		// Compares the glob pattern against
		// the file or directory name.
		FileVisitResult find(Path path) {
			Path name = path.getFileName();
			if (name != null && name.toString().equals(yamlConfigFilename)) {
				configPath = path;
				return FileVisitResult.TERMINATE;
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
	}

	public static class DBDataSource {

		private String dbDatabase;
		private String url;
		private String username;
		private String password;

		private DBDatabase createDBDatabase() throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			Class<?> dbDatabaseClass = Class.forName(this.getDbDatabase());
			String jdbcUrl = this.getUrl();
			String user = this.getUsername();
			String pass = this.getPassword();
			Constructor<?> constructor = dbDatabaseClass.getConstructor(String.class, String.class, String.class);
			Object newInstance = constructor.newInstance(jdbcUrl, user, pass);
			if (DBDatabase.class.isInstance(newInstance)) {
				return (DBDatabase) newInstance;
			} else {
				return null;
			}
		}

		/**
		 * @return the dbDatabase
		 */
		public String getDbDatabase() {
			return dbDatabase;
		}

		/**
		 * @return the url
		 */
		public String getUrl() {
			return url;
		}

		/**
		 * @return the username
		 */
		public String getUsername() {
			return username;
		}

		/**
		 * @return the password
		 */
		public String getPassword() {
			return password;
		}

		/**
		 * @param dbDatabase the dbDatabase to set
		 */
		public void setDbDatabase(String dbDatabase) {
			this.dbDatabase = dbDatabase;
		}

		/**
		 * @param url the url to set
		 */
		public void setUrl(String url) {
			this.url = url;
		}

		/**
		 * @param username the username to set
		 */
		public void setUsername(String username) {
			this.username = username;
		}

		/**
		 * @param password the password to set
		 */
		public void setPassword(String password) {
			this.password = password;
		}
	}

	public static class NoDatabaseConfigurationFound extends Exception {

		static final long serialVersionUID = 1L;

		private NoDatabaseConfigurationFound(String yamlConfigFilename) {
			super("No DBDatabase Configuration File named \"" + yamlConfigFilename + "\" was found in the filesystem: check the filname and ensure that the location is accessible from \".\"" + (Paths.get(".").toAbsolutePath()));
		}
	}

	public static class UnableToCreateDatabaseCluster extends Exception {

		static final long serialVersionUID = 1L;

		public UnableToCreateDatabaseCluster(Exception ex) {
			super("Unable Create DBDatabaseCluster Due To Exception", ex);
		}
	}
}
