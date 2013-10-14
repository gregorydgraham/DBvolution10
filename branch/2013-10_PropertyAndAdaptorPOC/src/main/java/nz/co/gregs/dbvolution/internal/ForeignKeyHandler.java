package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.DBRuntimeException;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with determining the table/class and column
 * referenced by this property.
 * This includes handling the {@link DBForeignKey} annotation on a class.
 * 
 * <p> This class behaves correctly when no {@link DBForeignKey} annotation is present.
 * @author Malcolm Lett
 */
class ForeignKeyHandler {
	private final Class<?> referencedClass;
	private final String referencedTableName;
	private final String referencedColumnName = null; // not yet supported
	private final DBForeignKey foreignKeyAnnotation; // null if not present on property
	
	public ForeignKeyHandler(JavaProperty adaptee) {
		this.foreignKeyAnnotation = adaptee.getAnnotation(DBForeignKey.class);
		
		// pre-calculate referenced class
		if (foreignKeyAnnotation != null) {
			this.referencedClass = foreignKeyAnnotation.value();
		}
		else {
			this.referencedClass = null;
		}

		// pre-calculate referenced table
		// (from annotation on referenced class)
		if (referencedClass != null) {
			TableHandler tableHandler = new TableHandler(referencedClass);
			if (!tableHandler.isTable()) {
				throw new DBPebkacException(adaptee.qualifiedName()+" is a foreign key to class "+referencedClass.getName()+
						", which is not a table");
			}
			if (tableHandler.getTableName() == null) {
				// not expected
				throw new DBRuntimeException(adaptee.qualifiedName()+" is a foreign key to class "+referencedClass.getName()+
						", which is a table but doesn't have a table name (this is probably a bug in DBvolution)");
			}
			
			this.referencedTableName = tableHandler.getTableName();
		}
		else {
			this.referencedTableName = null;
		}
	}

	/**
	 * Indicates whether this property references
	 * another class/table.
	 * @return
	 */
	public boolean isForeignKey() {
		return referencedClass != null;
	}
	
	/**
	 * Gets the class referenced by this foreign key.
	 * 
	 * <p> If the {@link DBForeignKey} annotation is missing, this method returns {@code null}.
	 * @return the referenced class or null if not a foreign key
	 */
	public Class<?> getReferencedClass() {
		return referencedClass;
	}
	
	/**
	 * Gets the name of the referenced table.
	 * 
	 * <p> If the {@link DBForeignKey} annotation is missing, this method returns {@code null}.
	 * @return the referenced table name, or null if not a foreign key
	 */
	public String getReferencedTableName() {
		return referencedTableName;
	}
	
	/**
	 * Gets the name of the referenced column in the referenced table, if known.
	 * The referenced column is only known if explicitly indicated, however in many cases
	 * it is not explicitly indicated.
	 * If the referenced column is not specified, the primary key of the referenced
	 * table should be referenced.
	 * 
	 * <p> Note: in the present implementation, the column name is never specified.
	 * 
	 * <p> If the {@link DBForeignKey} annotation is missing, this method returns {@code null}.
	 * @return the referenced column name; or null, indicating that the referenced table's primary key should be referenced
	 */
	public String getReferencedColumnName() {
		return referencedColumnName;
	}
	
	/**
	 * Gets the {@link DBColumn} annotation on the class, if it exists.
	 * @return the annotation or null if it is not present
	 */
	public DBForeignKey getDBForeignKeyAnnotation() {
		return foreignKeyAnnotation;
	}
}
