package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ArrayType;
import org.eclipse.jdt.core.dom.Name;
import org.eclipse.jdt.core.dom.ParameterizedType;
import org.eclipse.jdt.core.dom.PrimitiveType;
import org.eclipse.jdt.core.dom.PrimitiveType.Code;
import org.eclipse.jdt.core.dom.QualifiedType;
import org.eclipse.jdt.core.dom.SimpleType;
import org.eclipse.jdt.core.dom.Type;
import org.eclipse.jdt.core.dom.WildcardType;

/**
 * A reference to a type.
 * Models the type of a field, getter/setter method, type passed
 * to an annotation, or an annotation type itself.
 * At present excludes some AST types which are handled differently: imports
 */
public class ParsedTypeRef {
	private static final Class<?> UNRECOGNISED_JAVA_TYPE = ParsedTypeRef.class; // marker value
	private ParsedTypeContext typeContext;
	private Type astNode;
	private Class<?> javaType = null; // only if available

	/**
	 * Constructs a new type reference that is guaranteed to be imported by the context.
	 * @param typeContext
	 * @param type
	 * @return
	 */
	public static ParsedTypeRef newClassInstance(ParsedTypeContext typeContext, Class<?> type) {
		return new ParsedTypeRef(typeContext, typeContext.declarableTypeOf(type, true));
	}
	
	public ParsedTypeRef(ParsedTypeContext typeContext, Type astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
	}
	
	@Override
	public String toString() {
		return astNode.toString();
	}

	/**
	 * Hashcode based on string representation of referenced type.
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/**
	 * Equality based on string representation of referenced type.
	 * Only an approximate concept of equality that is suitable for use within
	 * the same source file.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ParsedTypeRef)) {
			return false;
		}
		ParsedTypeRef other = (ParsedTypeRef) obj;
		return this.toString().equals(other.toString());
	}

	public Type astNode() {
		return astNode;
	}
	
	public Name nameAstNode() {
		return ((SimpleType) astNode).getName();
	}
	
	/**
	 * Gets the name qualified to the level as it is declared.
	 * For complex types such as arrays and generic types, returns only
	 * the main referenced type.
	 */
	public String getDeclaredTypeName() {
		return fullyQualifiedDeclaredTypeNameOf(astNode);
	}

	/**
	 * Gets the simple name.
	 * For complex types such as arrays and generic types, returns only
	 * the main referenced type.
	 */
	public String getSimpleTypeName() {
		String name = fullyQualifiedDeclaredTypeNameOf(astNode);
		if (name.contains(".")) {
			return name.substring(name.lastIndexOf(".")+1);
		}
		return name;
	}
	
	/**
	 * Gets the (inferred) fully qualified name.
	 * For complex types such as arrays and generic types, returns only
	 * the main referenced type.
	 * This value returned by this method is inferred where possible,
	 * and left as the declared name when not possible due to ambiguous wildcard imports.
	 */
	public String getQualifiedTypeName() {
		return typeContext.getFullyQualifiedNameOf(
				fullyQualifiedDeclaredTypeNameOf(astNode));
	}
	
	/**
	 * Checks whether the referenced type is the specified java type
	 * object.
	 * Currently not aware of distinctions of array and parameterized
	 * types vs. simple types.
	 * @param type
	 * @return
	 */
	public boolean isJavaType(Class<?> type) {
		return typeContext.isDeclarationOfType(DBColumn.class, getDeclaredTypeName());
	}
	
	/**
	 * Gets the java type, if recognised.
	 * @return the type, if recognised, {@code null} otherwise.
	 * @deprecated not working yet
	 */
	@Deprecated
	public Class<?> getJavaTypeIfKnown() {
		if (javaType == null) {
			// TODO: calculate its value, use UNRECOGNISED_JAVA_TYPE if don't recognise it
		}
		return (javaType == UNRECOGNISED_JAVA_TYPE) ? null : javaType;
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
		return javaTypesOf(astNode);
	}
	
	/**
	 * Gets a string representing the declared type as it is declared,
	 * without frills such as array or parameterization indications.
	 * The returned string is qualified to the level that it is qualified
	 * in the source.
	 * @param type
	 * @return
	 */
	protected String fullyQualifiedDeclaredTypeNameOf(Type type) {
		Type rootType = rootTypeOf(type);
		if (rootType instanceof SimpleType) {
			return ((SimpleType) rootType).getName().getFullyQualifiedName();
		}
		else if (rootType instanceof PrimitiveType) {
			return ((PrimitiveType) rootType).getPrimitiveTypeCode().toString();
		}
		else if (rootType instanceof QualifiedType) {
			String qualifierName = fullyQualifiedDeclaredTypeNameOf(((QualifiedType) rootType).getQualifier());
			String simpleName = ((QualifiedType) rootType).getName().getFullyQualifiedName();
			if (qualifierName != null && !qualifierName.isEmpty()) {
				return qualifierName+"."+simpleName;
			}
			return simpleName;
		}
		throw new IllegalStateException("not prepared for "+astNode.getClass().getSimpleName()+" types");
	}
	
	/**
	 * Gets the single most important simple, primitive or qualified type.
	 * Handled as per:
	 * <ul>
	 * <li> simple type - the simple type reference
	 * <li> primitive type - the primitive type reference
	 * <li> array - the underlying element type (Integer[][] -&gt; Integer)
	 * <li> qualified type - the simple type without the qualifier (java.lang.Integer -&gt; Integer)
	 * <li> parameterized type - the simple type without its parameters (List<String> -&gt; List)
	 * </ul>
	 * @return
	 */
	protected Type rootTypeOf(Type type) {
		if (astNode instanceof SimpleType) {
			return (SimpleType) astNode;
		}
		else if (astNode instanceof ArrayType) {
			return rootTypeOf(((ArrayType) astNode).getElementType());
		}
		else if (astNode instanceof ParameterizedType) {
			return rootTypeOf(((ParameterizedType) astNode).getType());
		}
		else if (astNode instanceof PrimitiveType) {
			return astNode;
		}
		else if (astNode instanceof QualifiedType) {
			return astNode;
		}
		throw new IllegalStateException("not prepared for "+astNode.getClass().getSimpleName()+" types");
	}
	
	// TODO: this will never actually do anything useful because the handling 
	// of SimpleType is relied upon by every other type.
	private List<Class<?>> javaTypesOf(Type type) {
		if (type.isArrayType()) {
			return javaTypesOf(((ArrayType) type).getComponentType());
		}
		else if (type.isParameterizedType()) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			List<Class<?>> javaTypes = javaTypesOf(parameterizedType.getType());
			for (Type typeArg: (List<Type>) parameterizedType.typeArguments()) {
				javaTypes.addAll(javaTypesOf(typeArg));
			}
			return javaTypes;
		}
		else if (type.isPrimitiveType()) {
			Class<?> clazz = null;
			Code code = ((PrimitiveType) type).getPrimitiveTypeCode();
			if (code.equals(PrimitiveType.BOOLEAN)) {
				clazz = Boolean.class;
			}
			else if (code.equals(PrimitiveType.BYTE)) {
				clazz = Byte.class;
			}
			else if (code.equals(PrimitiveType.CHAR)) {
				clazz = Character.class;
			}
			else if (code.equals(PrimitiveType.DOUBLE)) {
				clazz = Double.class;
			}
			else if (code.equals(PrimitiveType.FLOAT)) {
				clazz = Float.class;
			}
			else if (code.equals(PrimitiveType.INT)) {
				clazz = Integer.class;
			}
			else if (code.equals(PrimitiveType.LONG)) {
				clazz = Long.class;
			}
			else if (code.equals(PrimitiveType.SHORT)) {
				clazz = Short.class;
			}
			else if (code.equals(PrimitiveType.VOID)) {
				clazz = Void.class;
			}
			else {
				throw new UnsupportedOperationException("Unrecognised PrimitiveType.Code: "+code);
			}
			List<Class<?>> javaTypes = new ArrayList<Class<?>>();
			javaTypes.add(clazz);
			return javaTypes;
		}
		else if (type.isQualifiedType()) {
			QualifiedType qualifiedType = (QualifiedType) type;
			List<Class<?>> javaTypes = javaTypesOf(qualifiedType.getQualifier());
			// not worrying about the qualified name (qualifiedType.getName()) for now
			return javaTypes;
		}
		else if (type.isSimpleType()) {
			SimpleType simpleType = (SimpleType) type;
			Name typeName = simpleType.getName();
			// TODO: need to do something intelligent with this, but what?
			// Can qualify the name via the imports, but I can't turn it into a Class<?> instance because
			// there's no reason that it has to be in my classpath.
			return new ArrayList<Class<?>>();
		}
		else if (type.isUnionType()) {
			throw new IllegalArgumentException("Union types not expected here");
		}
		else if (type.isWildcardType()) {
			WildcardType wildcard = (WildcardType) type;
			if (wildcard.getBound() != null) {
				return javaTypesOf(wildcard.getBound());
			}
			else {
				return new ArrayList<Class<?>>();
			}
		}
		else {
			throw new UnsupportedOperationException("Unrecognised Type type: "+type.getClass().getSimpleName()); 
		}
	}
}
