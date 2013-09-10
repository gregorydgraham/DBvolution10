package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a parsed field and/or parsed accessor methods into
 * one unit representing a standard bean property.
 * 
 * <p> The {@code ParsedBeanProperty} is very lenient in its handling of
 * semantic errors. Field type and accessor method types may disagree and
 * the accessor methods may not conform to bean specifications.
 * This allows for code inspection and modification even when
 * errors are present.
 */
public class ParsedBeanProperty {
	private ParsedField field = null;
	private ParsedMethod getter = null;
	private ParsedMethod setter = null;
	
	/**
	 * Creates a new property with a field and accessor methods.
	 * Updates the imports in the type context.
	 * 
	 * <p> Note: field name and method name duplication avoidance must be done outside of this method.
	 * @param typeContext
	 * @param propertyName
	 * @param propertyType
	 * @param isPrimaryKey
	 * @param columnName
	 * @return
	 */
	public static ParsedBeanProperty newDBColumnInstance(ParsedTypeContext typeContext, String propertyName, Class<?> propertyType, boolean isPrimaryKey, String columnName) {
		ParsedField field = ParsedField.newDBColumnInstance(typeContext,
				propertyName, propertyType, isPrimaryKey, columnName);
		
		ParsedMethod getter = ParsedMethod.newGetterInstance(typeContext, field);
		
		ParsedMethod setter = ParsedMethod.newSetterInstance(typeContext, field);
		
		return new ParsedBeanProperty(field, getter, setter);
	}
	
	public ParsedBeanProperty(ParsedField field, ParsedMethod getter, ParsedMethod setter) {
		this.field = field;
		this.getter = getter;
		this.setter = setter;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		if (field != null) {
			buf.append(field);
		}
		if (getter != null) {
			if (buf.length() > 0) buf.append("\n");
			buf.append(getter);
		}
		if (setter != null) {
			if (buf.length() > 0) buf.append("\n");
			buf.append(setter);
		}
		return buf.toString();
	}
	
	public ParsedField field() {
		return field;
	}

	public ParsedMethod getter() {
		return getter;
	}

	public ParsedMethod setter() {
		return setter;
	}

	/**
	 * Gets the type of the property.
	 * Where field and accessor methods disagree on property type,
	 * the field's type is used first, followed by the type of the getter.
	 * @return the type, or null if 
	 */
	public ParsedTypeRef getType() {
		if (field != null) {
			return field.getType();
		}
		if (getter != null) {
			ParsedTypeRef type = getter.getReturnType();
			if (type != null) {
				return type;
			}
		}
		if (setter != null) {
			if (setter.getArgumentTypes().size() == 1) {
				ParsedTypeRef type = setter.getArgumentTypes().get(0);
				if (type != null) {
					return type;
				}
			}
		}
		return null;
	}
	
	/**
	 * Gets all types that are referenced by the field declaration.
	 * For simple types, this is one value.
	 * For types with generics, this is one value plus one for each
	 * generic parameter.
	 * For recursive arrays, this can be any number of values.
	 * The resultant list can be used for constructing imports etc.
	 * @deprecated not working yet
	 * @return
	 */
	@Deprecated
	public List<Class<?>> getReferencedTypes() {
		return getType().getReferencedTypes();
	}
	
	/**
	 * Gets the name of the bean property, in camelCase.
	 * @return
	 */
	public String getName() {
		if (field != null) {
			return field.getName();
		}
		if (getter != null) {
			return JavaRules.propertyNameOf(getter);
		}
		if (setter != null) {
			return JavaRules.propertyNameOf(setter);
		}
		return null;
	}
	
	/**
	 * Gets the complete set of annotations attached to the field
	 * and/or getter/setter methods.
	 * @return
	 */
	public List<ParsedAnnotation> getAnnotations() {
		List<ParsedAnnotation> all = new ArrayList<ParsedAnnotation>();
		if (field != null) {
			all.addAll(field.getAnnotations());
		}
		if (getter != null) {
			all.addAll(getter.getAnnotations());
		}
		if (setter != null) {
			all.addAll(setter.getAnnotations());
		}
		return all;
	}
	
	/**
	 * Indicates whether this property is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBColumn} annotation
	 * on any of its field or getter/setter methods.
	 */
	public boolean isDBColumn() {
		return (field != null && field.isDBColumn()) ||
			   (getter != null && getter.isDBColumn()) ||
			   (setter != null && setter.isDBColumn());
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the property name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	// TODO need to be lenient if get annotations mixed up on field and accessors,
	// but not sure exactly what should be done.
	// TODO: arguably for case-insensitive databases, this should be using equalsIgnoreCase()
	public String getColumnNameIfSet() {
		if (field != null) {
			String columnName = field.getColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		if (getter != null) {
			String columnName = getter.getColumnNameIfSet();
//			if (col != null && columnName != null && !col.equals(columnName)) {
//				throw new IllegalArgumentException("Property "+getName()+" disagrees on column ("+columnName+" vs. "+col+")");
//			}
			if (columnName != null) {
				return columnName;
			}
		}
		if (setter != null) {
			String columnName = setter.getColumnNameIfSet();
//			if (col != null && columnName != null && !col.equals(columnName)) {
//				throw new IllegalArgumentException("Property "+getName()+" disagrees on column ("+columnName+" vs. "+col+")");
//			}
			if (columnName != null) {
				return columnName;
			}
		}
		return null;
	}
	
	public void setField(ParsedField field) {
		this.field = field;
	}

	public void setGetter(ParsedMethod getter) {
		this.getter = getter;
	}

	public void setSetter(ParsedMethod setter) {
		this.setter = setter;
	}	
	
}
