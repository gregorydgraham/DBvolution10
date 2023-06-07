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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
//import javax.tools.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/**
 *
 * @author gregorygraham
 */
public class DataRepo {

//	private final DBDatabase database;
	private final List<DBTableClass> views = new ArrayList<DBTableClass>(0);
	private final List<DBTableClass> tables = new ArrayList<DBTableClass>(0);
	final List<DBRow> rows = new ArrayList<>(0);
//	private final String packageName;

	public static DataRepo getDataRepoFor(DBDatabase database, String packageName) throws SQLException, IOException {
		var repo = DataRepoGenerator.generateClasses(database, packageName);
		return repo;
	}
	private final Options options;

	public DataRepo(Options options) {
		this.options = options.copy();
	}

	void addViews(List<DBTableClass> generatedViews) {
		this.views.addAll(generatedViews);
	}

	void addTables(List<DBTableClass> generatedTables) {
		this.tables.addAll(generatedTables);
	}

	void addView(DBTableClass generatedView) {
		this.views.add(generatedView);
	}

	void addTable(DBTableClass generatedTable) {
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

	public List<DBTableClass> getAllKnownEntities() {
		List<DBTableClass> knownEntities = new ArrayList<DBTableClass>(0);
		knownEntities.addAll(views);
		knownEntities.addAll(tables);
		return knownEntities;
	}

	public DBDatabase getDatabase() {
		return options.getDBDatabase();
	}

	public List<DBRow> getRows() {
		List<DBRow> knownEntities = new ArrayList<DBRow>(0);
		knownEntities.addAll(rows);
		return knownEntities;
	}

	void compile(Options options) {
		final List<DBTableClass> knownEntities = getAllKnownEntities();
		DBRowSubclassGenerator.generate(knownEntities, options);
		for (DBTableClass t : knownEntities) {
			rows.add(t.getGeneratedInstance());
		}
	}

	public DBRow getInstanceForName(String className) {
		for (DBRow row : rows) {
			if (row.getClass().getSimpleName().equals(className)) {
				return row;
			} else if (row.getClass().getCanonicalName().equals(className)) {
				return row;
			}
		}
		return null;
	}

	/**
	 * @return the packageName
	 */
	public String getPackageName() {
		return options.getPackageName();
	}

}
