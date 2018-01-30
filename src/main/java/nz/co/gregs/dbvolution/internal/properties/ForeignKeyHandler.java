package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.ReferenceToUndefinedPrimaryKeyException;
import nz.co.gregs.dbvolution.exceptions.UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.DamerauLevenshtein;
import org.simmetrics.simplifiers.Simplifiers;
import static org.simmetrics.builders.StringMetricBuilder.with;

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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
// TODO if referenced property has differing case of column name,
// need to throw exception during a deferred validation step once database case-ness is known.
class ForeignKeyHandler implements Serializable{

	private static final long serialVersionUID = 1l;

	private final boolean identityOnly;
	private final Class<? extends DBRow> referencedClass;
	private final PropertyWrapperDefinition identityOnlyReferencedProperty; // stores identity info only
	private transient final DBForeignKey foreignKeyAnnotation; // null if not present on property
	private final ColumnHandler originalColumn;

	ForeignKeyHandler(JavaProperty adaptee, boolean processIdentityOnly) {
		this.originalColumn = new ColumnHandler(adaptee);
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
				throw new ReferenceToUndefinedPrimaryKeyException(adaptee.qualifiedName() + " is a foreign key to class " + referencedClassWrapper.javaName()
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
					throw new ReferenceToUndefinedPrimaryKeyException(adaptee.qualifiedName() + " references column " + declaredReferencedColumnName
							+ ", however there are " + properties.size() + " such properties in " + referencedClassWrapper.javaName() + ".");
				}
				if (properties.isEmpty()) {
					throw new ReferenceToUndefinedPrimaryKeyException("Property " + adaptee.qualifiedName() + " references class " + referencedClassWrapper.javaName()
							+ " and column " + declaredReferencedColumnName + ", but the column doesn't exist");
				}

				// get explicitly identified property
				identifiedReferencedProperty = properties.get(0);
			}

			// validate: referenced class has single primary key when implicitly referencing primary key column
			if (declaredReferencedColumnName == null) {
				PropertyWrapperDefinition[] primaryKeys = referencedClassWrapper.primaryKeyDefinitions();
				if (primaryKeys == null || primaryKeys.length == 0) {
					throw new ReferenceToUndefinedPrimaryKeyException(adaptee, referencedClassWrapper);
				} else if (primaryKeys.length > 1) {
					final String columnName = originalColumn.getColumnName();
					StringMetric metric = with(new DamerauLevenshtein())
							.simplify(Simplifiers.replaceNonWord())
							.simplify(Simplifiers.toLowerCase())
							.build();
					Map<Float, PropertyWrapperDefinition> pkComps = new HashMap<>();
//					Map<PropertyWrapperDefinition, Float> pkMetrics = new HashMap<>();
					Float maxComp = 0.0F;

					for (PropertyWrapperDefinition primaryKey : primaryKeys) {
						final String pkName = primaryKey.getColumnName();
						float result = metric.compare(columnName, pkName);
						pkComps.put(result, primaryKey);
//						pkMetrics.put(primaryKey,result);
						maxComp = maxComp > result ? maxComp : result;
					}
					if (maxComp <= 0.15F) {
						throw new UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException(adaptee, referencedClassWrapper, primaryKeys);
					} else {
						identifiedReferencedProperty = pkComps.get(maxComp);
					}
				} else {
					identifiedReferencedProperty = primaryKeys[0];
				}
			}
		}
		this.identityOnlyReferencedProperty = identifiedReferencedProperty;
	}

	/**
	 * Indicates whether this property references another class/table.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
