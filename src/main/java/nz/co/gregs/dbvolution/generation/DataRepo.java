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
package nz.co.gregs.dbvolution.generation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.tools.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DataRepo {

	private final DBDatabase database;
	private final List<DBTableClass> views = new ArrayList<DBTableClass>(0);
	private final List<DBTableClass> tables = new ArrayList<DBTableClass>(0);
	private Boolean compiled = false;
	private final List<DBRow> rows = new ArrayList<>(0);
	private final String packageName;
	private File classLocation;

	public static DataRepo getDataRepoFor(DBDatabase database, String packageName) throws SQLException, IOException {
		var repo = DBTableClassGenerator.generateClassesOfViewsAndTables(database, packageName);
		repo.compile();
		return repo;
	}

	DataRepo(DBDatabase db, String packageName) {
		this.database = db;
		this.packageName = packageName;
	}

	public void addViews(List<DBTableClass> generatedViews) {
		this.compiled = false;
		this.views.addAll(generatedViews);
	}

	public void addTables(List<DBTableClass> generatedTables) {
		this.compiled = false;
		this.tables.addAll(generatedTables);
	}

	public void addView(DBTableClass generatedView) {
		this.compiled = false;
		this.views.add(generatedView);
	}

	public void addTable(DBTableClass generatedTable) {
		this.compiled = false;
		this.tables.add(generatedTable);
	}

	public List<DBTableClass> getTables() {
		List<DBTableClass> knownEntities = new ArrayList<DBTableClass>(0);
		knownEntities.addAll(tables);
		return knownEntities;
	}

	public List<DBTableClass> getViews() {
		List<DBTableClass> knownEntities = new ArrayList<DBTableClass>(0);
		knownEntities.addAll(views);
		return knownEntities;
	}

	public List<DBTableClass> getKnownEntities() {
		List<DBTableClass> knownEntities = new ArrayList<DBTableClass>(0);
		knownEntities.addAll(views);
		knownEntities.addAll(tables);
		return knownEntities;
	}

	public DBDatabase getDatabase() {
		return database;
	}

	void compile() throws IOException {
		if (!compiled) {
			List<JavaSourceFromString> compilationUnits = new ArrayList<JavaSourceFromString>(); // input for first compilation task
			for (DBTableClass dbcl : getKnownEntities()) {
				compilationUnits.add(new JavaSourceFromString(dbcl.getFullyQualifiedName(), dbcl.getJavaSource()));
			}
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
			Boolean succeeded;
			// Try to add the classes to the TARGET directory
			try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
				// Try to add the classes to the TARGET directory
				List<File> locations = new ArrayList<File>();
				File file = new File(System.getProperty("user.dir"), "target");
				if (!file.exists()) {
					file = new File(System.getProperty("user.dir"));
				}
				final File saveLocation = file;
				this.classLocation = saveLocation;
				locations.add(saveLocation);
				fileManager.setLocation(StandardLocation.CLASS_OUTPUT, locations);
				JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits);
				succeeded = task.call();
				for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
					succeeded = false;
					System.out.println("DIAGNOSTIC: " + diagnostic);
				}
				if (succeeded) {
					for (JavaSourceFromString compilationUnit : compilationUnits) {
						try {
							final DBRow instance = getInstance(compilationUnit);
							rows.add(instance);
						} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | IOException ex) {
							System.out.println("ERR: " + ex.getLocalizedMessage());
							Logger.getLogger(DataRepo.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
				}
			}
			this.compiled = succeeded;
		}
	}

	private DBRow getInstance(JavaSourceFromString v) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
		Class<?> classFromFileSystem = getClassFromFileSystem(v);
		final DBRow instanceFromClass = getInstanceFromClass(classFromFileSystem);
		return instanceFromClass;
	}

	private Class<?> getClassFromFileSystem(JavaSourceFromString v) throws IOException, ClassNotFoundException {
		// Convert File to a URL
		URL startingLocationURL = this.classLocation.toURI().toURL();
		URL[] startingLocationURLs = new URL[]{startingLocationURL};

		// Create a new class loader with the directory
		ClassLoader cl = new URLClassLoader(startingLocationURLs);

		// Having set the place to start looking for the class earlier,
		// we can now look for the class
		Class<?> cls = cl.loadClass(v.fullyQualifiedName);
		return cls;
	}

	public List<DBRow> getRows() {
		try {
			compile();
		} catch (IOException ex) {
			System.out.println("DATAREPO ERR: "+ex.getLocalizedMessage());
			Logger.getLogger(DataRepo.class.getName()).log(Level.SEVERE, null, ex);
		}
		List<DBRow> knownEntities = new ArrayList<DBRow>(0);
		knownEntities.addAll(rows);
		return knownEntities;
	}

	private DBRow getInstanceFromClass(Class<?> classFromFileSystem) throws NoSuchMethodException, InvocationTargetException, IllegalArgumentException, IllegalAccessException, InstantiationException {
		Constructor<?> constructor = classFromFileSystem.getConstructor();
		constructor.setAccessible(true);
		Object newInstance = constructor.newInstance();
		return (DBRow) newInstance;
	}

	/**
	 * A file object used to represent source coming from a string.
	 */
	public class JavaSourceFromString extends SimpleJavaFileObject {

		/**
		 * The source code of this "file".
		 */
		final String code;
		private final String fullyQualifiedName;

		/**
		 * Constructs a new JavaSourceFromString.
		 *
		 * @param name the fullyQualifiedName of the compilation unit represented by
		 * this file object
		 * @param code the source code for the compilation unit represented by this
		 * file object
		 */
		JavaSourceFromString(String name, String code) {
			super(URI.create("string:///" + name.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension),
					JavaFileObject.Kind.SOURCE);
			this.fullyQualifiedName = name;
			this.code = code;
		}

		@Override
		public CharSequence getCharContent(boolean ignoreEncodingErrors) {
			return code;
		}

		public String getFullyQualifiedName() {
			return fullyQualifiedName;
		}
	}

	/**
	 * @return the packageName
	 */
	public String getPackageName() {
		return packageName;
	}

}
