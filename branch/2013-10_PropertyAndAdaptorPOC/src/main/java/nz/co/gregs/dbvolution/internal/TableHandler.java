package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.annotations.DBTableName;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with determining the table to which a class should be mapped.
 * This includes handling the {@link DBTableName} annotation on a class.
 * 
 * <p> This class behaves correctly when no {@link DBTableName} annotation is present.
 * @author Malcolm Lett
 */
// TODO: is the handling here regarding no annotation correct? Or are all classes valid as a table?
class TableHandler {
	private final String tableName;
	private final DBTableName tableNameAnnotation; // null if not present on class
	
	public TableHandler(Class<?> adaptee) {
		this.tableNameAnnotation = adaptee.getAnnotation(DBTableName.class);
		
		// pre-calculate table name
		// (null if no annotation, default if annotation present but no name given)
		if (tableNameAnnotation != null) {
			String name = tableNameAnnotation.value();
			this.tableName = (name == null || name.trim().equals("")) ? adaptee.getSimpleName() : name;
		} else {
			this.tableName = null;
		}
	}

	/**
	 * Indicates whether this class maps to a database table.
	 * @return
	 */
	public boolean isTable() {
		return tableName != null;
	}
	
	/**
	 * Gets the explicitly or implicitly indicated table name.
	 * Defaulted to the value of the class if {@link DBTableName} annotation is present
	 * but doesn't explicitly specify the table name.
	 * 
	 * <p> If the {@link DBTableName} annotation is missing, this method returns {@code null}.
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Gets the {@link DBTableName} annotation on the class, if it exists.
	 * @return the annotation or null if it is not present
	 */
	public DBTableName getDBTableNameAnnotation() {
		return tableNameAnnotation;
	}
}
