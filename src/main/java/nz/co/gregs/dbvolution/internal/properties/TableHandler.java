package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import nz.co.gregs.dbvolution.annotations.DBRequiredTable;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with determining the table to which a class
 * should be mapped. This includes handling the {@link DBTableName} annotation
 * on a class.
 *
 * <p>
 * This class behaves correctly when no {@link DBTableName} annotation is
 * present.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
class TableHandler implements Serializable{

	private static final long serialVersionUID = 1l;

	private final boolean isTable;
	private final String tableName;
	private transient final DBTableName tableNameAnnotation; // null if not present on class
	private transient final DBSelectQuery selectQueryAnnotation; // null if not present on class
	private transient final Object requiredTableAnnotation;

	public TableHandler(Class<?> adaptee) {
		this.tableNameAnnotation = adaptee.getAnnotation(DBTableName.class);
		this.selectQueryAnnotation = adaptee.getAnnotation(DBSelectQuery.class);
		this.requiredTableAnnotation = adaptee.getAnnotation(DBRequiredTable.class);

		// must extend DBRow to be a table
		this.isTable = DBRow.class.isAssignableFrom(adaptee);

		this.tableName = deriveTableName(adaptee);
	}

	private String deriveTableName(Class<?> adaptee) {
		// pre-calculate table name
		// (default if no annotation present or name not given)
		if (adaptee.getSuperclass().equals(Object.class)) {
			return adaptee.getSimpleName();
		} else if (adaptee.getSuperclass().getSuperclass().equals(Object.class)) {
			return adaptee.getSimpleName();
		} else if (adaptee.getSuperclass().getSuperclass().equals(RowDefinition.class)) {
			String explicitName = null;
			if (tableNameAnnotation != null) {
				String name = tableNameAnnotation.value();
				if (name != null && !name.trim().isEmpty()) {
					explicitName = name;
				}
			}
			return (explicitName == null) ? adaptee.getSimpleName() : explicitName;
		} else {
			return deriveTableName(adaptee.getSuperclass());
		}
	}

	/**
	 * Indicates whether this class maps to a database table. {@code true} always
	 * with the present implementation.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the class represents a DB table, otherwise FALSE.
	 */
	public boolean isTable() {
		return isTable;
	}

	/**
	 * Gets the explicitly or implicitly indicated table name. Defaulted to the
	 * value of the class if {@link DBTableName} annotation is present but doesn't
	 * explicitly specify the table name.
	 *
	 * <p>
	 * If the {@link DBTableName} annotation is missing, this method returns
	 * {@code null}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String getTableName() {
		return isTable ? tableName : null;
	}

	/**
	 * Gets the explicitly or implicitly indicated table name. Defaulted to the
	 * value of the class if {@link DBTableName} annotation is present but doesn't
	 * explicitly specify the table name.
	 *
	 * <p>
	 * If the {@link DBTableName} annotation is missing, this method returns
	 * {@code null}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String getSelectQuery() {
		final DBSelectQuery dbSelectQueryAnnotation = getDBSelectQueryAnnotation();
		return isTable&&(dbSelectQueryAnnotation!=null) ? dbSelectQueryAnnotation.value() : null;
	}

	/**
	 * Gets the {@link DBTableName} annotation on the class, if it exists.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the annotation or null if it is not present
	 */
	public DBTableName getDBTableNameAnnotation() {
		return tableNameAnnotation;
	}

	/**
	 * Gets the {@link DBSelectQuery} annotation on the class, if it exists.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the annotation or null if it is not present
	 */
	public DBSelectQuery getDBSelectQueryAnnotation() {
		return selectQueryAnnotation;
	}

	String getSchemaName() {
		final DBTableName dbTableNameAnnotation = getDBTableNameAnnotation();
		if (dbTableNameAnnotation == null) {
			return "";
		} else {
			return dbTableNameAnnotation.schema();
		}
	}

	boolean isRequiredTable() {
		return requiredTableAnnotation!=null;
	}
}
