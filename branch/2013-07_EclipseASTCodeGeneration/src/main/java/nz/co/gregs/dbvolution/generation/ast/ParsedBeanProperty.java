package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
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
	private ParsedPropertyMember annotatedMember = null;
	
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
		ParsedField field;
		if (typeContext.getConfig().isAnnotateFields()) {
			field = ParsedField.newDBColumnInstance(typeContext,
					propertyName, propertyType, isPrimaryKey, columnName);
		}
		else {
			field = ParsedField.newInstance(typeContext, propertyName, propertyType, false);
		}
		
		ParsedMethod getter = null;
		ParsedMethod setter = null;
		if (typeContext.getConfig().isGenerateAccessorMethods()) {
			getter = ParsedMethod.newGetterInstance(typeContext, field);
			setter = ParsedMethod.newSetterInstance(typeContext, field);
			
			if (!typeContext.getConfig().isAnnotateFields()) {
				getter.addAnnotation(ParsedAnnotation.newDBColumnInstance(typeContext, columnName));
				if (isPrimaryKey) {
					getter.addAnnotation(ParsedAnnotation.newDBPrimaryKeyInstance(typeContext));
				}
			}
		}
		
		return new ParsedBeanProperty(field, getter, setter);
	}
	
	public ParsedBeanProperty(ParsedField field, ParsedMethod getter, ParsedMethod setter) {
		this.field = field;
		this.getter = getter;
		this.setter = setter;
		this.annotatedMember = selectedAnnotatedMember(field, getter, setter);
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
	
	public ParsedTypeContext getTypeContext() {
		if (field != null) {
			return field.getTypeContext();
		}
		if (getter != null) {
			return getter.getTypeContext();
		}
		if (setter != null) {
			return setter.getTypeContext();
		}
		return null;
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
	
	public void setField(ParsedField field) {
		this.field = field;
	}

	public void setGetter(ParsedMethod getter) {
		this.getter = getter;
	}

	public void setSetter(ParsedMethod setter) {
		this.setter = setter;
	}	

	/**
	 * Gets the single primary annotated member of this bean property.
	 * This is the member which will have annotations added to it
	 * and which will be used for inferring property name and type.
	 * The member will not necessarily have any annotations at the time
	 * this method is called.
	 * 
	 * <p> The rules for choosing the member are:
	 * <ul> 
	 * <li> inherit the preference from the existing use of the DBColumn annotation,
	 *      if present, or any DBvolution annotations otherwise
	 * <li> give preference to fields or accessor methods based on the
	 *      code generation configuration (where not otherwise indicated by other rules)
	 * <li> prefer getter over setter methods (where not otherwise indicated by other rules)
	 * <li> where multiple members are annotated, use the above rules to pick just one
	 * </ul>
	 * @param the field or accessor method that is best suited for holding new annotations
	 */
	protected ParsedPropertyMember annotatedMember() {
		return annotatedMember;
	}
	
	/**
	 * Gets the type of the property.
	 * Where field and accessor methods disagree on property type,
	 * the field's type is used first, followed by the type of the getter.
	 * @return the type, or null if can't be inferred
	 */
	public ParsedTypeRef getType() {
		ParsedPropertyMember member = annotatedMember();
		
		// get type from preferred member only
		if (member != null) {
			if (member instanceof ParsedField) {
				return ((ParsedField) member).getType();
			}
			else if (member instanceof ParsedMethod && ((ParsedMethod) member).isGetter()) {
				return ((ParsedMethod) member).getReturnType();
			}
			else if (member instanceof ParsedMethod && ((ParsedMethod) member).isSetter()) {
				ParsedMethod method = (ParsedMethod) member;
				if (method.getArgumentTypes().size() == 1) {
					return method.getArgumentTypes().get(0);
				}
			}
		}
		return null;
	}
	
	/**
	 * Gets the name of the bean property, in camelCase.
	 * @return the property name, can be null in rare circumstances
	 */
	public String getName() {
		ParsedPropertyMember member = annotatedMember();
		
		// get name from preferred member first
		String name = null;
		if (member != null) {
			if (member instanceof ParsedField) {
				name = ((ParsedField) member).getName();
			}
			else {
				name = JavaRules.propertyNameOf((ParsedMethod) member);
			}
		}
		if (name != null) {
			return name;
		}
		
		// get first name found
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
	 * Adds the annotation to the end of the list of annotations
	 * that are shown on the line before the field or method starts.
	 * @param annotation the annotation to add
	 */
	public void addAnnotation(ParsedAnnotation annotation) {
		if (annotatedMember() != null) {
			annotatedMember().addAnnotation(annotation);
		}
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
	 * Indicates whether this property is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBColumn} annotation
	 * on any of its field or getter/setter methods.
	 */
	public boolean isDBPrimaryKey() {
		return (field != null && field.isDBPrimaryKey()) ||
			   (getter != null && getter.isDBPrimaryKey()) ||
			   (setter != null && setter.isDBPrimaryKey());
	}

	/**
	 * Indicates whether this property is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBColumn} annotation
	 * on any of its field or getter/setter methods.
	 */
	public boolean isDBForeignKey() {
		return (field != null && field.isDBForeignKey()) ||
			   (getter != null && getter.isDBForeignKey()) ||
			   (setter != null && setter.isDBForeignKey());
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the property name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		if (field != null) {
			String columnName = field.getColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		if (getter != null) {
			String columnName = getter.getColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		if (setter != null) {
			String columnName = setter.getColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		return null;
	}

	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the property name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public ParsedTypeRef getForeignTypeIfSet() {
		if (field != null) {
			ParsedTypeRef type = field.getForeignTypeIfSet();
			if (type != null) {
				return type;
			}
		}
		if (getter != null) {
			ParsedTypeRef type = getter.getForeignTypeIfSet();
			if (type != null) {
				return type;
			}
		}
		if (setter != null) {
			ParsedTypeRef type = setter.getForeignTypeIfSet();
			if (type != null) {
				return type;
			}
		}
		return null;
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the property name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getForeignColumnNameIfSet() {
		if (field != null) {
			String columnName = field.getForeignColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		if (getter != null) {
			String columnName = getter.getForeignColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		if (setter != null) {
			String columnName = setter.getForeignColumnNameIfSet();
			if (columnName != null) {
				return columnName;
			}
		}
		return null;
	}

	/**
	 * Gets the single primary annotated member of this bean property,
	 * which is the member which will have annotations added to it.
	 * The member will not necessarily have any annotations at the time
	 * this method is called.
	 * 
	 * <p> The rules for choosing the member are:
	 * <ul> 
	 * <li> inherit the preference from the existing use of the DBColumn annotation,
	 *      if present, or any DBvolution annotations otherwise
	 * <li> give preference to fields or accessor methods based on the
	 *      code generation configuration (where not otherwise indicated by other rules)
	 * <li> prefer getter over setter methods (where not otherwise indicated by other rules)
	 * <li> where multiple members are annotated, use the above rules to pick just one
	 * </ul>
	 * @param field
	 * @param getter
	 * @param setter
	 * @return the field or accessor method that is best suited for holding new annotations
	 */
	protected static ParsedPropertyMember selectedAnnotatedMember(ParsedField field, ParsedMethod getter, ParsedMethod setter) {
		// start with all members and whittle down
		List<ParsedPropertyMember> availableMembers = new LinkedList<ParsedPropertyMember>();
		if (field != null) {
			availableMembers.add(field);
		}
		if (getter != null) {
			availableMembers.add(getter);
		}
		if (setter != null) {
			availableMembers.add(setter);
		}

		// prefer only those with DBColumn annotations if any
		boolean columnIsAvailable = false;
		for (ParsedPropertyMember member: availableMembers) {
			if (member.isDBColumn()) {
				columnIsAvailable = true;
				break;
			}
		}
		if (columnIsAvailable) {
			Iterator<ParsedPropertyMember> itr = availableMembers.iterator();
			while (itr.hasNext()) {
				ParsedPropertyMember member = itr.next();
				if (!member.isDBColumn()) {
					itr.remove();
				}
			}
		}
		
		// otherwise prefer only those with DBvolution annotations if any
		if (!columnIsAvailable) {
			boolean dbvAnnotationIsAvailable = false;
			for (ParsedPropertyMember member: availableMembers) {
				if (hasDBvolutionAnnotations(member)) {
					dbvAnnotationIsAvailable = true;
					break;
				}
			}
			if (dbvAnnotationIsAvailable) {
				Iterator<ParsedPropertyMember> itr = availableMembers.iterator();
				while (itr.hasNext()) {
					ParsedPropertyMember member = itr.next();
					if (!hasDBvolutionAnnotations(member)) {
						itr.remove();
					}
				}
			}
		}
		
		// choose field vs. accessor by code generation configuration
		ParsedTypeContext typeContext =
				(field != null) ? field.getTypeContext() :
				(getter != null) ? getter.getTypeContext() :
				setter.getTypeContext();
				
		if (typeContext.getConfig().isAnnotateFields()) {
			boolean fieldIsAvailable = false;
			for (ParsedPropertyMember member: availableMembers) {
				if (member instanceof ParsedField) {
					fieldIsAvailable = true;
					break;
				}
			}
			
			// remove accessors if contains a field
			if (fieldIsAvailable) {
				Iterator<ParsedPropertyMember> itr = availableMembers.iterator();
				while (itr.hasNext()) {
					ParsedPropertyMember member = itr.next();
					if (member instanceof ParsedMethod) {
						itr.remove();
					}
				}
			}
		}
		else {
			boolean accessorIsAvailable = false;
			for (ParsedPropertyMember member: availableMembers) {
				if (member instanceof ParsedMethod) {
					accessorIsAvailable = true;
					break;
				}
			}
			
			// remove fields if contains an accessor
			if (accessorIsAvailable) {
				Iterator<ParsedPropertyMember> itr = availableMembers.iterator();
				while (itr.hasNext()) {
					ParsedPropertyMember member = itr.next();
					if (member instanceof ParsedField) {
						itr.remove();
					}
				}
			}
		}
		
		// choose getter over setter
		boolean getterIsAvailable = false;
		for (ParsedPropertyMember member: availableMembers) {
			if (member instanceof ParsedMethod && ((ParsedMethod) member).isGetter()) {
				getterIsAvailable = true;
				break;
			}
		}
		if (getterIsAvailable) {
			Iterator<ParsedPropertyMember> itr = availableMembers.iterator();
			while (itr.hasNext()) {
				ParsedPropertyMember member = itr.next();
				if (member instanceof ParsedMethod && !((ParsedMethod) member).isGetter()) {
					itr.remove();
				}
			}
		}
		
		// pick one of the remaining
		if (!availableMembers.isEmpty()) {
			return availableMembers.get(0);
		}
		return null;
	}
	
	protected static boolean hasDBvolutionAnnotations(ParsedPropertyMember member) {
		for (ParsedAnnotation annotation: member.getAnnotations()) {
			for (Class<?> expectedType: ParsedAnnotation.DBVOLUTION_ANNOTATION_TYPES) {
				if (annotation.isType(expectedType)) {
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Unifies the concepts of fields and accessor methods for executing
	 * operations where it doesn't matter what the underlying type of member is.
	 */
	public static interface ParsedPropertyMember {
		/**
		 * Gets the set of annotations attached to the field or accessor.
		 */
		public List<ParsedAnnotation> getAnnotations();
		
		/**
		 * Adds the annotation to the end of the list of annotations
		 * that are shown on the line before the method starts.
		 * @param annotation the annotation to add
		 */
		public void addAnnotation(ParsedAnnotation annotation);
		
		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
		 */
		public boolean isDBColumn();
		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
		 */
		public boolean isDBPrimaryKey();
		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
		 */
		public boolean isDBForeignKey();		
		/**
		 * Gets the column name, as specified via the {@code DBColumn} annotation
		 * or defaulted based on the field name, if it has a {@code DBColumn}
		 * annotation.
		 * @return {@code null} if not applicable
		 */
		public String getColumnNameIfSet();
		/**
		 * Gets the foreign key's referenced class, as specified via the {@code DBForeignKey} annotation.
		 * @return {@code null} if not applicable
		 */
		public ParsedTypeRef getForeignTypeIfSet();
		/**
		 * Gets the foreign key's referenced column, as specified via the {@code DBForeignKey} annotation.
		 * @return {@code null} if not applicable
		 */
		public String getForeignColumnNameIfSet();		
	}
}
