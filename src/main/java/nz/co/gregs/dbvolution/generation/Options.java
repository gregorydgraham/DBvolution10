/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

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

	public Options() {
	}

	public Options(Long versionNumber, PrimaryKeyRecognisor pkRecog, ForeignKeyRecognisor fkRecog, Boolean trimCharColumns) {
		this.versionNumber = versionNumber;
		this.pkRecog = pkRecog;
		this.fkRecog = fkRecog;
		this.trimCharColumns = trimCharColumns;
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
	 */
	public void setVersionNumber(Long versionNumber) {
		this.versionNumber = versionNumber;
	}

	/**
	 * @param pkRecog the pkRecog to set
	 */
	public void setPkRecog(PrimaryKeyRecognisor pkRecog) {
		this.pkRecog = pkRecog;
	}

	/**
	 * @param fkRecog the fkRecog to set
	 */
	public void setFkRecog(ForeignKeyRecognisor fkRecog) {
		this.fkRecog = fkRecog;
	}

	/**
	 * @param trimCharColumns the trimCharColumns to set
	 */
	public void setTrimCharColumns(Boolean trimCharColumns) {
		this.trimCharColumns = trimCharColumns;
	}

	/**
	 * @param includeForeignKeyColumnName the includeForeignKeyColumnName to set
	 */
	public void setIncludeForeignKeyColumnName(Boolean includeForeignKeyColumnName) {
		this.includeForeignKeyColumnName = includeForeignKeyColumnName;
	}
	
}
