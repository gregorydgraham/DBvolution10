package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the property as a column that may or may
 * not be a primary key. This includes handling the {@link DBColumn} and
 * {@link DBPrimaryKey} annotations on the property, but excludes handling its
 * type.
 *
 * <p>
 * This class behaves correctly when no {@link DBColumn} annotation is present.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
class ColumnHandler  implements Serializable{

	private static final long serialVersionUID = 1l;

	private final String columnName;
	private transient final DBColumn columnAnnotation; // null if not present on property
	private transient final DBPrimaryKey primaryKeyAnnotation; // null if not present on property
	private transient final DBAutoIncrement autoIncrementAnnotation; // null if not present on property

	ColumnHandler(JavaProperty adaptee) {
		this.columnAnnotation = adaptee.getAnnotation(DBColumn.class);
		this.primaryKeyAnnotation = adaptee.getAnnotation(DBPrimaryKey.class);
		this.autoIncrementAnnotation = adaptee.getAnnotation(DBAutoIncrement.class);

		// pre-calculate column name
		// (null if no annotation, default if annotation present but no name given)
		if (columnAnnotation != null) {
			String name = columnAnnotation.value();
			this.columnName = (name == null || name.trim().isEmpty()) ? adaptee.name() : name;
		} else {
			this.columnName = null;
		}
	}

	/**
	 * Indicates whether this property maps to a database column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if a column
	 */
	public boolean isColumn() {
		return columnAnnotation != null;
	}

	/**
	 * Indicates whether this property is a primary key column.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@code true} if a column and marked as primary key
	 */
	public boolean isPrimaryKey() {
		return isColumn() && (primaryKeyAnnotation != null);
	}

	/**
	 * Gets the explicitly or implicitly indicated column name.
	 *
	 * <p>
	 * Defaults to the name of the field if the {@link DBColumn} annotation is
	 * present but doesn't explicitly specify the column name.
	 *
	 * <p>
	 * If the {@link DBColumn} annotation is missing, this method returns
	 * {@code null}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the column name, if {@code DBColumn} annotation is present, or
	 * {@code null}.
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * Gets the {@link DBColumn} annotation on the class, if it exists.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the annotation or null if it is not present
	 */
	public DBColumn getDBColumnAnnotation() {
		return columnAnnotation;
	}

	boolean isAutoIncrement() {
		return this.autoIncrementAnnotation != null;
	}
}
