package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the property as a column that may or may not
 * be a primary key.
 * This includes handling the {@link DBColumn} and {@link DBPrimaryKey} annotations
 * on the property, but excludes handling its type.
 * 
 * <p> This class behaves correctly when no {@link DBColumn} annotation is present.
 * @author Malcolm Lett
 */
class ColumnHandler {
	private final String columnName;
	private final DBColumn columnAnnotation; // null if not present on property
	private final DBPrimaryKey primaryKeyAnnotation; // null if not present on property
	
	public ColumnHandler(JavaProperty adaptee) {
		this.columnAnnotation = adaptee.getAnnotation(DBColumn.class);
		this.primaryKeyAnnotation = adaptee.getAnnotation(DBPrimaryKey.class);
		
		// pre-calculate column name
		// (null if no annotation, default if annotation present but no name given)
		if (columnAnnotation != null) {
			String name = columnAnnotation.value();
			this.columnName = (name == null || name.trim().equals("")) ? adaptee.name() : name;
		} else {
			this.columnName = null;
		}
	}

	/**
	 * Indicates whether this property maps to a database column.
	 * @return {@code true} if a column
	 */
	public boolean isColumn() {
		return columnAnnotation != null;
	}
	
	/**
	 * Indicates whether this property is a primary key column.
	 * @return {@code true} if a column and marked as primary key
	 */
	public boolean isPrimaryKey() {
		return isColumn() && (primaryKeyAnnotation != null);
	}
	
	/**
	 * Gets the explicitly or implicitly indicated column name.
	 * Defaulted to the value of the class if {@link DBColumn} annotation is present
	 * but doesn't explicitly specify the column name.
	 * 
	 * <p> If the {@link DBColumn} annotation is missing, this method returns {@code null}.
	 * @return the column name, if specified explicitly or implicitly.
	 */
	public String getColumnName() {
		return columnName;
	}
	
	/**
	 * Gets the {@link DBColumn} annotation on the class, if it exists.
	 * @return the annotation or null if it is not present
	 */
	public DBColumn getDBColumnAnnotation() {
		return columnAnnotation;
	}
}
