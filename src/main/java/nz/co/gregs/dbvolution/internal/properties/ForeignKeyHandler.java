package nz.co.gregs.dbvolution.internal.properties;

import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with determining the table/class and column
 * referenced by this property. This includes handling the {@link DBForeignKey}
 * annotation on a class.
 *
 * <p>
 * This class behaves correctly when no {@link DBForeignKey} annotation is
 * present.
 *
 * @author Malcolm Lett
 */
// TODO if referenced property has differing case of column name,
// need to throw exception during a deferred validation step once database case-ness is known.
class ForeignKeyHandler {

	private final boolean identityOnly;
	private final Class<? extends DBRow> referencedClass;
	private final PropertyWrapperDefinition identityOnlyReferencedProperty; // stores identity info only
	private final DBForeignKey foreignKeyAnnotation; // null if not present on property

	ForeignKeyHandler(JavaProperty adaptee, boolean processIdentityOnly) {
		if (processIdentityOnly) {
			// skip processing of foreign keys
			this.foreignKeyAnnotation = null;
			this.identityOnly = true;
		} else {
			this.foreignKeyAnnotation = adaptee.getAnnotation(DBForeignKey.class);
			this.identityOnly = false;
		}

		// pre-calculate referenced class
		if (foreignKeyAnnotation != null) {
			this.referencedClass = foreignKeyAnnotation.value();
		} else {
			this.referencedClass = null;
		}

		// pre-calculate declared referenced column
		String declaredReferencedColumnName = null;
		if (foreignKeyAnnotation != null) {
			if (foreignKeyAnnotation.column() != null && foreignKeyAnnotation.column().trim().length() > 0) {
				declaredReferencedColumnName = foreignKeyAnnotation.column();
			}
		}

		// pre-calculate referenced property
		// (from annotations etc. on referenced class)
		PropertyWrapperDefinition identifiedReferencedProperty = null;
		if (referencedClass != null) {
			RowDefinitionClassWrapper referencedClassWrapper = new RowDefinitionClassWrapper(referencedClass, true);

			// validate: referenced class is valid enough for the purposes of doing queries on this class
			if (!referencedClassWrapper.isTable()) {
				throw new DBPebkacException(adaptee.qualifiedName() + " is a foreign key to class " + referencedClassWrapper.javaName()
						+ ", which is not a table");
			}
			if (referencedClassWrapper.tableName() == null) {
				// not expected
				throw new DBRuntimeException(adaptee.qualifiedName() + " is a foreign key to class " + referencedClassWrapper.javaName()
						+ ", which is a table but doesn't have a table name (this is probably a bug in DBvolution)");
			}

			// validate: explicitly declared column name exists on referenced table
			if (declaredReferencedColumnName != null) {
				List<PropertyWrapperDefinition> properties = referencedClassWrapper.getPropertyDefinitionIdentitiesByColumnNameCaseInsensitive(declaredReferencedColumnName);
				if (properties.size() > 1) {
					throw new DBPebkacException(adaptee.qualifiedName() + " references column " + declaredReferencedColumnName
							+ ", however there are " + properties.size() + " such properties in " + referencedClassWrapper.javaName() + ".");
				}
				if (properties.isEmpty()) {
					throw new DBPebkacException("Property " + adaptee.qualifiedName() + " references class " + referencedClassWrapper.javaName()
							+ " and column " + declaredReferencedColumnName + ", but the column doesn't exist");
				}

				// get explicitly identified property
				identifiedReferencedProperty = properties.get(0);
			}

			// validate: referenced class has single primary key when implicitly referencing primary key column
			if (declaredReferencedColumnName == null) {
				PropertyWrapperDefinition primaryKey = referencedClassWrapper.primaryKeyDefinition();
				if (primaryKey == null) {
					throw new DBPebkacException("Property " + adaptee.qualifiedName() + " references class " + referencedClassWrapper.javaName()
							+ ", which does not have a primary key. Please identify the primary key on that class or specify the column in the"
							+ " @" + DBForeignKey.class.getSimpleName() + " declaration.");
				}
//				else {
//					// TODO once support multiple primary keys
//					if (primaryKeyProperties.size() > 1) {
//						throw new DBPebkacException("Property "+qualifiedJavaName()+" references class "+referencedClassWrapper.javaName()
//								+" using an implicit primary key reference, but the referenced class has "+primaryKeyProperties.size()
//								+" primary key columns. You must include explicit foreign column names.");
//					}
//				}
				identifiedReferencedProperty = primaryKey;
			}
		}
		this.identityOnlyReferencedProperty = identifiedReferencedProperty;
	}

	/**
	 * Indicates whether this property references another class/table.
	 *
	 * @return TRUE/FALSE
	 */
	public boolean isForeignKey() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return referencedClass != null;
	}

	/**
	 * Gets the class referenced by this foreign key.
	 *
	 * @return the referenced class if this property is a foreign key; null if not
	 * a foreign key
	 */
	public Class<? extends DBRow> getReferencedClass() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return referencedClass;
	}

	/**
	 * Gets the name of the referenced table.
	 *
	 * @return the referenced table name if this property is a foreign key; null
	 * if not a foreign key
	 */
	public String getReferencedTableName() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return (identityOnlyReferencedProperty == null) ? null : identityOnlyReferencedProperty.tableName();
	}

	/**
	 * Gets the name of the referenced column in the referenced table. The
	 * referenced column is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the single
	 * primary key of the referenced table.
	 *
	 * @return the referenced column name if this property is a foreign key; null
	 * if not a foreign key
	 */
	public String getReferencedColumnName() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return (identityOnlyReferencedProperty == null) ? null : identityOnlyReferencedProperty.getColumnName();
	}

	/**
	 * Gets identity information for the referenced property in the referenced
	 * table. The referenced property is either explicitly indicated by use of the
	 * {@link DBForeignKey#column()} attribute, or it is implicitly the single
	 * primary key of the referenced table.
	 *
	 * <p>
	 * Note that the property definition returned provides identity of the
	 * property only. It provides access to the property's java name, column name,
	 * type, and identity information about the table it belongs to (ie the table
	 * name). Attempts to get or set its value or get the type adaptor instance
	 * will result in an internal exception.
	 *
	 * @return the referenced property if this property is a foreign key; null if
	 * not a foreign key
	 */
	public PropertyWrapperDefinition getReferencedPropertyDefinitionIdentity() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return identityOnlyReferencedProperty;
	}

	/**
	 * Gets the {@link DBColumn} annotation on the class, if it exists.
	 *
	 * @return the annotation or null if it is not present
	 */
	public DBForeignKey getDBForeignKeyAnnotation() {
		if (identityOnly) {
			throw new AssertionError("Attempt to access non-identity information of identity-only foreign key handler");
		}
		return foreignKeyAnnotation;
	}

	boolean isForeignKeyTo(Class<? extends DBRow> aClass) {
		final Class<? extends DBRow> reffedClass = getReferencedClass();
		final boolean assignableFrom = reffedClass.isAssignableFrom(aClass);
		return assignableFrom;
	}
}
