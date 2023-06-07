/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.util.Arrays;
import nz.co.gregs.dbvolution.databases.DBDatabase;

/*
 * @param fkRecog fkRecog an object that can recognize foreign key columns by
 * the column name and derive the related table
 * @param versionNumber versionNumber
 * @param pkRecog pkRecog an object that can recognize primary key columns by
 * the column name
 * @param trimCharColumns
 */
public class Options {

	private Long versionNumber = 1l;
	private PrimaryKeyRecognisor pkRecog = new PrimaryKeyRecognisor();
	private ForeignKeyRecognisor fkRecog = new ForeignKeyRecognisor();
	private Boolean trimCharColumns = false;
	private Boolean includeForeignKeyColumnName = false;
	private DBDatabase database;
	private String packageName;
	private String[] objectTypes;

	public Options() {
	}

	public static Options empty() {
		return new Options();
	}

	public Options(DBDatabase database, String packageName, Long versionNumber, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, Boolean trimCharColumns, Boolean includeForeignKeyColumnName, String... objectTypes) {
		this.database = database;
		this.packageName = packageName;
		this.versionNumber = versionNumber;
		this.pkRecog = pkRecog;
		this.fkRecog = fkRecog;
		this.trimCharColumns = trimCharColumns;
		this.includeForeignKeyColumnName = includeForeignKeyColumnName;
		this.objectTypes = objectTypes;
	}

	public Options(Long versionNumber, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, Boolean trimCharColumns) {
		this(null, "", versionNumber, pkRecog, fkRecog, trimCharColumns, false, new String[0]);
	}

	/**
	 * @return the versionNumber
	 */
	public Long getVersionNumber() {
		return versionNumber;
	}

	/**
	 * @return the pkRecog
	 */
	public PrimaryKeyRecognisor getPkRecog() {
		return pkRecog;
	}

	/**
	 * @return the fkRecog
	 */
	public ForeignKeyRecognisor getFkRecog() {
		return fkRecog;
	}

	/**
	 * @return the trimCharColumns
	 */
	public Boolean getTrimCharColumns() {
		return trimCharColumns;
	}

	/**
	 * @return the includeForeignKeyColumnName
	 */
	public Boolean getIncludeForeignKeyColumnName() {
		return includeForeignKeyColumnName;
	}

	/**
	 * @param versionNumber the versionNumber to set
	 * @return this instance
	 */
	public Options setVersionNumber(Long versionNumber) {
		this.versionNumber = versionNumber;
		return this;
	}

	/**
	 * @param pkRecog the pkRecog to set
	 * @return this instance
	 */
	public Options setPkRecog(PrimaryKeyRecognisor pkRecog) {
		this.pkRecog = pkRecog;
		return this;
	}

	/**
	 * @param fkRecog the fkRecog to set
	 * @return this instance
	 */
	public Options setFkRecog(ForeignKeyRecognisor fkRecog) {
		this.fkRecog = fkRecog;
		return this;
	}

	/**
	 * @param trimCharColumns the trimCharColumns to set
	 * @return this instance
	 */
	public Options setTrimCharColumns(Boolean trimCharColumns) {
		this.trimCharColumns = trimCharColumns;
		return this;
	}

	/**
	 * @param includeForeignKeyColumnName the includeForeignKeyColumnName to set
	 * @return this instance
	 */
	public Options setIncludeForeignKeyColumnName(Boolean includeForeignKeyColumnName) {
		this.includeForeignKeyColumnName = includeForeignKeyColumnName;
		return this;
	}

	public Options setDBDatabase(DBDatabase database) {
		this.database = database;
		return this;
	}

	public Options setPackageName(String packageName) {
		this.packageName = packageName;
		return this;
	}

	public String getPackageName() {
		return packageName;
	}

	public DBDatabase getDBDatabase() {
		return database;
	}

	public Options copy() {
		Options opts = new Options(database, packageName, versionNumber, pkRecog, fkRecog, trimCharColumns, includeForeignKeyColumnName, objectTypes);
		return opts;
	}

	public static Options copy(Options opts) {
		Options newOpts = opts.copy();
		return newOpts;
	}

	public Options setObjectTypes(String... dbObjectTypes) {
		if (dbObjectTypes.length > 0) {
			this.objectTypes = Arrays.copyOf(dbObjectTypes, dbObjectTypes.length);
		} else {
			this.objectTypes = new String[0];
		}
		return this;
	}

	public String[] getObjectTypes() {
		return this.objectTypes;
	}

}
