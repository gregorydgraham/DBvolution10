package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.Assignment;
import org.eclipse.jdt.core.dom.ExpressionStatement;
import org.eclipse.jdt.core.dom.FieldAccess;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.Modifier;
import org.eclipse.jdt.core.dom.PrimitiveType;
import org.eclipse.jdt.core.dom.ReturnStatement;
import org.eclipse.jdt.core.dom.SingleVariableDeclaration;
import org.eclipse.jdt.core.dom.Type;

/**
 * The parsed details of a member method within a class.
 * @author Malcolm Lett
 */
public class ParsedMethod {
	private ParsedTypeContext typeContext;
	private ParsedTypeRef returnType;
	private List<ParsedTypeRef> argumentTypes;
	private MethodDeclaration astNode;
	private List<ParsedAnnotation> annotations;
	
	/**
	 * Creates a standard getter method for the given field.
	 * @param typeContext
	 * @param field
	 * @return the new method, ready to be added to the java type
	 */
	public static ParsedMethod newGetterInstance(ParsedTypeContext typeContext, ParsedField field) {
		AST ast = typeContext.getAST();
		
		String methodName = JavaRules.getterMethodNameForField(field);
		
		// add imports: TODO
		// (field may not be actually defined in this class, so still need to check imports)
		//boolean fieldTypeImported = typeContext.ensureImport(field.); // TODO: need ParsedField.getType() to work for this
		
		// add method
		MethodDeclaration method = ast.newMethodDeclaration();
		method.setName(ast.newSimpleName(methodName));
		method.setReturnType2((Type) ASTNode.copySubtree(ast, field.astNode().getType()));
		
		// add body
		ReturnStatement returnStatement = ast.newReturnStatement();
		returnStatement.setExpression(ast.newSimpleName(field.getName()));
		method.setBody(ast.newBlock());
		method.getBody().statements().add(returnStatement);
		
		// set visibility modifiers
		method.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		
		// add javadoc
		method.setJavadoc(ParsedJavadoc.astMethodInstance(typeContext, null, null, "the "+field.getName()));
				
		return new ParsedMethod(typeContext, method);
	}

	/**
	 * Creates a standard setter method for the given field.
	 * @param typeContext
	 * @param field
	 * @return the new method, ready to be added to the java type
	 */
	public static ParsedMethod newSetterInstance(ParsedTypeContext typeContext, ParsedField field) {
		AST ast = typeContext.getAST();
		
		String methodName = JavaRules.setterMethodNameForField(field);

		// add imports: TODO
		// (field may not be actually defined in this class, so still need to check imports)
		//boolean fieldTypeImported = typeContext.ensureImport(field.); // TODO: need ParsedField.getType() to work for this
		
		// add method
		MethodDeclaration method = ast.newMethodDeclaration();
		method.setName(ast.newSimpleName(methodName));
		method.setReturnType2(ast.newPrimitiveType(PrimitiveType.VOID));
		SingleVariableDeclaration parameter = ast.newSingleVariableDeclaration();
		parameter.setType((Type) ASTNode.copySubtree(ast, field.astNode().getType()));
		parameter.setName(ast.newSimpleName(field.getName()));
		method.parameters().add(parameter);
		
		// add body
		FieldAccess fieldAccess = ast.newFieldAccess();
		fieldAccess.setExpression(ast.newThisExpression());
		fieldAccess.setName(ast.newSimpleName(field.getName()));
		Assignment assignment = ast.newAssignment();
		assignment.setLeftHandSide(fieldAccess);
		assignment.setOperator(Assignment.Operator.ASSIGN);
		assignment.setRightHandSide(ast.newSimpleName(parameter.getName().getFullyQualifiedName()));
		ExpressionStatement setStatement = ast.newExpressionStatement(assignment);
		method.setBody(ast.newBlock());
		method.getBody().statements().add(setStatement);
		
		// set visibility modifiers
		method.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		
		// add javadoc
		method.setJavadoc(ParsedJavadoc.astMethodInstance(typeContext, null,
				new String[][] {
					new String[]{parameter.getName().getFullyQualifiedName(),
							"the "+parameter.getName().getFullyQualifiedName()+" to set"}
				},
				null));
		
		return new ParsedMethod(typeContext, method);
	}
	
	/**
	 * Gets the getter method for the given field, if it is available.
	 * @param field must be a field within this class or supertype.
	 * @return the identified method or null if not found
	 */
	public static ParsedMethod findGetterFor(ParsedField field, ParsedClass parsedClass) {
		// phase 1a: direct bean property
		ParsedMethod method = parsedClass.getMethod(JavaRules.getterMethodNameForField(field));
		if (method != null && method.isGetter()) {
			return method;
		}
		
		// phase 1b: 'get' instead of 'is'
		String phase2MethodName = JavaRules.getterMethodNameForField(field);
		if (phase2MethodName.startsWith("is")) {
			phase2MethodName = "get" + phase2MethodName.substring("is".length());
			method = parsedClass.getMethod(phase2MethodName);
			if (method != null && method.isGetter()) {
				return method;
			}
		}
		
		// TODO - phase 2: case-insensitive match?
		return null;
	}

	/**
	 * Gets the setter method for the given field, if it is available.
	 * @param field must be a field within this class or supertype.
	 * @return the identified method or null if not found
	 */
	public static ParsedMethod findSetterFor(ParsedField field, ParsedClass parsedClass) {
		// phase 1: direct bean property
		ParsedMethod method = parsedClass.getMethod(JavaRules.setterMethodNameForField(field));
		if (method != null && method.isSetter()) {
			return method;
		}
		
		// TODO - phase 2: case-insensitive match?
		return null;
	}

	/**
	 * Gets the getter method that matches the given setter method, if it is available.
	 * Finds the getter method using a basic name match or via the field.
	 * @param setter
	 * @param parsedClass
	 * @return
	 */
	public static ParsedMethod findGetterFor(ParsedMethod setter, ParsedClass parsedClass) {
		// phase 1a: direct method name correlation
		String propertyName = JavaRules.propertyNameOf(setter);
		String getterName = JavaRules.getterMethodNameForField(propertyName, null);
		ParsedMethod method = parsedClass.getMethod(getterName);
		if (method != null && method.isGetter()) {
			return method;
		}

		// phase 1b: 'is' instead of 'get'
		if (getterName.startsWith("get")) {
			String phase2MethodName = "is" + getterName.substring("get".length());
			method = parsedClass.getMethod(phase2MethodName);
			if (method != null && method.isGetter()) {
				return method;
			}
		}
		
		// phase 2: via field
		ParsedField field = parsedClass.getField(propertyName);
		if (field != null) {
			method = parsedClass.getGetterMethodFor(field);
			if (method != null && method.isGetter()) {
				return method;
			}
		}
		
		return null;
	}
	
	/**
	 * Gets the setter method that matches the given getter method, if it is available.
	 * Finds the setter method using a basic name match or via the field.
	 * @param getter
	 * @param parsedClass
	 * @return
	 */
	public static ParsedMethod findSetterFor(ParsedMethod getter, ParsedClass parsedClass) {
		// phase 1: direct method name correlation
		String propertyName = JavaRules.propertyNameOf(getter);
		String setterName = JavaRules.setterMethodNameForField(propertyName, null);
		ParsedMethod method = parsedClass.getMethod(setterName);
		if (method != null && method.isSetter()) {
			return method;
		}
		
		// phase 2: via field
		ParsedField field = parsedClass.getField(propertyName);
		if (field != null) {
			method = parsedClass.getSetterMethodFor(field);
			if (method != null && method.isSetter()) {
				return method;
			}
		}
		
		return null;
	}

	public ParsedMethod(ParsedTypeContext typeContext, MethodDeclaration astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
		
		// return type
		this.returnType = new ParsedTypeRef(typeContext, astNode.getReturnType2());
		
		// argument types
		this.argumentTypes = new ArrayList<ParsedTypeRef>();
		for (SingleVariableDeclaration varDecl: (List<SingleVariableDeclaration>)astNode.parameters()) {
			argumentTypes.add(new ParsedTypeRef(typeContext, varDecl.getType()));
		}
		
    	// method annotations
		this.annotations = new ArrayList<ParsedAnnotation>();
    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
    		if (modifier.isAnnotation()) {
    			annotations.add(new ParsedAnnotation(typeContext, (Annotation)modifier));
    		}
    	}
	}

	/**
	 * Returns a nominal representation of a method, without body.
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (ParsedAnnotation annotation: annotations) {
			buf.append(annotation).append("\n");
		}
		buf.append("method ");
		if (getReturnType() == null) {
			buf.append("void ");
		}
		else {
			buf.append(getReturnType().toString()).append(" ");
		}
		buf.append(getName());
		buf.append("(");
		if (getArgumentTypes() != null) {
			boolean first = true;
			for (ParsedTypeRef argType: getArgumentTypes()) {
				if (!first) buf.append(",");
				buf.append(argType.toString());
				first = false;
			}
		}
		buf.append(");");
		return buf.toString();
	}
	
	public MethodDeclaration astNode() {
		return astNode;
	}
	
	public ParsedTypeRef getReturnType() {
		return returnType;
	}
	
	public List<ParsedTypeRef> getArgumentTypes() {
		return argumentTypes;
	}
	
	public String getName() {
		return astNode.getName().getFullyQualifiedName();
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return annotations;
	}
	
	/**
	 * Indicates whether this is a standard getter method.
	 * Requires that it takes no arguments and returns
	 * a type.
	 * @return
	 */
	public boolean isGetter() {
		String name = getName();
		if (name.startsWith("get") && name.length() > "get".length()) {
			// ok, continue
		}
		else if (name.startsWith("is") && name.length() > "is".length()) {
			// ok, continue
		}
		else {
			return false;
		}
		
		if (!astNode.parameters().isEmpty()) {
			return false;
		}
		if (astNode.getReturnType2() instanceof PrimitiveType &&
				((PrimitiveType) astNode.getReturnType2()).getPrimitiveTypeCode().equals(PrimitiveType.VOID)) {
			return false;
		}
		return true;
	}

	/**
	 * Indicates whether this is a standard setter method.
	 * Requires that it takes one argument.
	 * Return type is ignored.
	 * @return
	 */
	public boolean isSetter() {
		String name = getName();
		if (name.startsWith("set") && name.length() > "set".length()) {
			// ok, continue
		}
		else {
			return false;
		}
		
		if (astNode.parameters().size() != 1) {
			return false;
		}
		return true;
	}
	
	/**
	 * Indicates whether this method is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBColumn} annotation.
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
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the field name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBColumn()) {
				String columnName = annotation.getColumnNameIfSet();
				if (columnName == null) {
					columnName = JavaRules.propertyNameOf(this); // default based on property name
				}
				return columnName;
			}
		}
		return null;
	}
}
