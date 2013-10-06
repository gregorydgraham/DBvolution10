package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty.ParsedPropertyMember;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.ClassInstanceCreation;
import org.eclipse.jdt.core.dom.FieldDeclaration;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.Modifier;
import org.eclipse.jdt.core.dom.Type;
import org.eclipse.jdt.core.dom.VariableDeclarationFragment;

/**
 * The parsed details of an member field within a class.
 * 
 * <p> Member field declarations can specify multiple variables within the same declaration;
 * while only specifying the type once, and the annotations once.
 * This type transparently handles that and exposes the variables and independent
 * fields.
 */
public class ParsedField implements ParsedPropertyMember {
	private ParsedFieldDeclaration parsedFieldDeclaration;
	private String name;
	
	/**
	 * Creates a new field and prepares the type context for addition of the field.
	 * Updates the imports in the type context.
	 * 
	 * <p> Note: field name duplication avoidance must be done outside of this method.
	 * @param typeContext
	 * @param fieldName
	 * @param fieldType
	 * @param isPrimaryKey
	 * @param columnName
	 * @return
	 */
	public static ParsedField newDBColumnInstance(ParsedTypeContext typeContext, String fieldName, Class<?> fieldType, boolean isPrimaryKey, String columnName) {
		AST ast = typeContext.getAST();
		
		ParsedField field = newInstance(typeContext, fieldName, fieldType, true);
		FieldDeclaration fieldDecl = field.astNode();
		VariableDeclarationFragment variable = (VariableDeclarationFragment) fieldDecl.fragments().get(0);
		
		// add annotations (to top, in reverse order)
		fieldDecl.modifiers().add(0,
				ParsedAnnotation.newDBColumnInstance(typeContext, columnName).astNode());
		if (isPrimaryKey) {
			fieldDecl.modifiers().add(0,
					ParsedAnnotation.newDBPrimaryKeyInstance(typeContext).astNode());
		}

		// add initialisation section
		ClassInstanceCreation initializer = ast.newClassInstanceCreation();
		initializer.setType((Type) ASTNode.copySubtree(ast, fieldDecl.getType()));
		variable.setInitializer(initializer);
		
		return field;
	}
	
	/**
	 * Creates a new field and prepares the type context for addition of the field.
	 * Updates the imports in the type context.
	 * <p> Note: field name duplication avoidance must be done outside of this method.
	 * @param typeContext
	 * @param fieldName
	 * @param fieldType
	 * @param isPublic whether to make the field public, else private
	 * @return
	 */
	public static ParsedField newInstance(ParsedTypeContext typeContext, String fieldName, Class<?> fieldType, boolean isPublic) {
		AST ast = typeContext.getAST();
		
		// add field
		VariableDeclarationFragment variable = ast.newVariableDeclarationFragment();
		variable.setName(ast.newSimpleName(fieldName));
		FieldDeclaration field = ast.newFieldDeclaration(variable);
		field.setType(typeContext.declarableTypeOf(fieldType, true));
		
		// set visibility modifiers
		if (isPublic) {
			field.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		}
		else {
			field.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PRIVATE_KEYWORD));
		}
		
		// wrap with domain-specific types
		ParsedFieldDeclaration parsedFieldDeclaration = new ParsedFieldDeclaration(typeContext, field);
		if (parsedFieldDeclaration.getFields().isEmpty()) {
			throw new AssertionError("Internal logic error: expected 1 field, got none");
		}
		else if (parsedFieldDeclaration.getFields().size() > 1) {
			throw new AssertionError("Internal logic error: expected 1 field, got "+parsedFieldDeclaration.getFields().size());
		}
		return parsedFieldDeclaration.getFields().get(0);
	}

	/**
	 * Construct new instances for each field variable declared by the field declaration.
	 * @param typeContext
	 * @param astNode
	 * @return
	 */
	public static List<ParsedField> of(ParsedTypeContext typeContext, FieldDeclaration astNode) {
		return new ParsedFieldDeclaration(typeContext, astNode).getFields();
	}

	/**
	 * Used internally only for a single field variable.
	 * @param parsedFieldDeclaration
	 * @param name
	 */
	private ParsedField(ParsedFieldDeclaration parsedFieldDeclaration, VariableDeclarationFragment variableDeclaration) {
		this.parsedFieldDeclaration = parsedFieldDeclaration;
		this.name = variableDeclaration.getName().getFullyQualifiedName();
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (ParsedAnnotation annotation: getAnnotations()) {
			buf.append(annotation).append("\n");
		}
		buf.append("field "+getType()+" "+getName());
		buf.append(";");
		if (isDBColumn()) {
			buf.append(" // columnName="+getColumnNameIfSet());
		}
		return buf.toString();
	}
	
	public FieldDeclaration astNode() {
		return parsedFieldDeclaration.astNode();
	}
	
	public ParsedTypeContext getTypeContext() {
		return parsedFieldDeclaration.typeContext;
	}
	
	public ParsedTypeRef getType() {
		return parsedFieldDeclaration.getType();
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
	
	public String getName() {
		return name;
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return parsedFieldDeclaration.getAnnotations();
	}
	
	/**
	 * Adds the annotation to the end of the list of annotations
	 * that are shown on the line before the method starts.
	 * @param annotation the annotation to add
	 */
	public void addAnnotation(ParsedAnnotation annotation) {
		parsedFieldDeclaration.addAnnotation(annotation);
	}
	
	/**
	 * Indicates whether this field is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
	 */
	public boolean isDBColumn() {
		return parsedFieldDeclaration.isDBColumn();
	}

	/**
	 * Indicates whether this field is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
	 */
	public boolean isDBPrimaryKey() {
		return parsedFieldDeclaration.isDBPrimaryKey();
	}

	/**
	 * Indicates whether this field is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBTableColumn} annotation.
	 */
	public boolean isDBForeignKey() {
		return parsedFieldDeclaration.isDBForeignKey();
	}
	
	/**
	 * Gets the column name, as specified via the {@code DBColumn} annotation
	 * or defaulted based on the field name, if it has a {@code DBColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBColumn()) {
				String columnName = annotation.asDBColumn().getColumnNameIfSet();
				if (columnName == null) {
					// defaulting mechanism
					columnName = getName();
				}
				return columnName;
			}
		}
		return null;
	}

	/**
	 * Gets the foreign key's referenced class, as specified via the {@code DBForeignKey} annotation.
	 * @return {@code null} if not applicable
	 */
	public ParsedTypeRef getForeignTypeIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBForeignKey()) {
				return annotation.asDBForeignKey().getReferencedClassIfSet();
			}
		}
		return null;
	}

	/**
	 * Gets the foreign key's referenced column, as specified via the {@code DBForeignKey} annotation.
	 * @return {@code null} if not applicable
	 */
	public String getForeignColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBForeignKey()) {
				return annotation.asDBForeignKey().getReferencedColumnNameIfSet();
			}
		}
		return null;
	}
	
	// used by toString() method only
	private static String joinNamesOf(List<ParsedField> fields, String delimiter) {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (ParsedField field: fields) {
			if (!first) buf.append(delimiter);
			first = false;
			
			buf.append(field.getName());
		}
		return buf.toString();
	}
	
	// used by toString() method only
	private static String join(List<String> strings, String delimiter) {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (String str: strings) {
			if (!first) buf.append(delimiter);
			first = false;
			
			buf.append(str);
		}
		return buf.toString();
	}
	
	/**
	 * Models the actual field declaration within the source file that
	 * contains one or more variable declarations, and zero or more
	 * shared annotations.
	 */
	public static class ParsedFieldDeclaration {
		private ParsedTypeContext typeContext;
		private FieldDeclaration astNode;
		private ParsedTypeRef type;
		private List<ParsedAnnotation> annotations;
		private List<ParsedField> fields; // one or more variables within the field declaration
		
		public ParsedFieldDeclaration(ParsedTypeContext typeContext, FieldDeclaration astNode) {
			this.typeContext = typeContext;
			this.astNode = astNode;
			
			// field type
			this.type = new ParsedTypeRef(typeContext, astNode.getType());

	    	// field annotations
			this.annotations = new ArrayList<ParsedAnnotation>();
	    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
	    		if (modifier.isAnnotation()) {
	    			annotations.add(new ParsedAnnotation(typeContext, (Annotation)modifier));
	    		}
	    	}		
			
			// field names
			this.fields = new ArrayList<ParsedField>();
	    	for (VariableDeclarationFragment variable: (List<VariableDeclarationFragment>)astNode.fragments()) {
	    		fields.add(new ParsedField(this, variable));
	    	}
		}
		
		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			for (ParsedAnnotation annotation: getAnnotations()) {
				buf.append(annotation).append("\n");
			}
			buf.append("field "+joinNamesOf(getFields(), ", "));
			buf.append(";");
			if (isDBColumn()) {
				buf.append(" // columnNames="+join(getColumnNamesIfSet(),","));
			}
			return buf.toString();
		}
		
		public FieldDeclaration astNode() {
			return astNode;
		}
		
		public ParsedTypeRef getType() {
			return type;
		}
		
		public List<ParsedAnnotation> getAnnotations() {
			return annotations;
		}
		
		public List<ParsedField> getFields() {
			return fields;
		}
		
		/**
		 * Adds the annotation to the end of the list of annotations
		 * that are shown on the line before the method starts.
		 * @param annotation the annotation to add
		 */
		public void addAnnotation(ParsedAnnotation annotation) {
			int i = 0, target = 0;
	    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
	    		if (modifier.isAnnotation()) {
	    			target=i+1;
	    		}
	    		i++;
	    	}
	    	
	    	astNode.modifiers().add(target, annotation.astNode());
	    	annotations.add(annotation);
		}
		
		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBColumn}
		 * annotation.
		 */
		public boolean isDBColumn() {
			for (ParsedAnnotation annotation: getAnnotations()) {
				if (annotation.isDBColumn()) {
					return true;
				}
			}
			return false;
		}

		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBPrimaryKey}
		 * annotation.
		 */
		public boolean isDBPrimaryKey() {
			for (ParsedAnnotation annotation: getAnnotations()) {
				if (annotation.isDBPrimaryKey()) {
					return true;
				}
			}
			return false;
		}

		/**
		 * Indicates whether this field is declared with a
		 * {@link nz.co.gregs.dbvolution.annotations.DBForeignKey}
		 * annotation.
		 */
		public boolean isDBForeignKey() {
			for (ParsedAnnotation annotation: getAnnotations()) {
				if (annotation.isDBForeignKey()) {
					return true;
				}
			}
			return false;
		}
		
		/**
		 * Gets the column names, as specified via the {@code DBColumn} annotation
		 * or defaulted based on the field names, if it has a {@code DBColumn}
		 * annotation.
		 * Really just here for use by the {@link #toString()} method.
		 * @return {@code null} if not applicable
		 */
		public List<String> getColumnNamesIfSet() {
			Set<String> uniqueNames = new LinkedHashSet<String>(); // retains order
			for (ParsedField field: fields) {
				String name = field.getColumnNameIfSet();
				if (name != null) {
					uniqueNames.add(name);
				}
			}
			return new ArrayList<String>(uniqueNames);
		}
	}
}
