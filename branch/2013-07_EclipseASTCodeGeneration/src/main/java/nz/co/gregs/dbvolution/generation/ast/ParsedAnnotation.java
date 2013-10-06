package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.Expression;
import org.eclipse.jdt.core.dom.MemberValuePair;
import org.eclipse.jdt.core.dom.NormalAnnotation;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;
import org.eclipse.jdt.core.dom.Type;
import org.eclipse.jdt.core.dom.TypeLiteral;

/**
 * Represents an annotation in a source file associated with any
 * source file component.
 */
public class ParsedAnnotation {
	/** Known DBvolution annotation types */
	public static final Class<?>[] DBVOLUTION_ANNOTATION_TYPES = new Class<?>[] {
			DBTableName.class, DBColumn.class, DBPrimaryKey.class,
			DBForeignKey.class
		};
	
	private ParsedTypeContext typeContext;
	private Annotation astNode;

	/**
	 * Creates a new annotation instance to mark a database table.
	 * The annotation is not added to the AST and must be added by the caller;
	 * however any referenced types are added to the imports section of the 
	 * type context.
	 * @param typeContext
	 * @param tableName explicitly specified table name,
	 * null to leave empty and imply the use of a default table name
	 * @return the newly constructed instance
	 */
	public static ParsedAnnotation newDBTableInstance(ParsedTypeContext typeContext, String tableName) {
		return ParsedDBTableNameAnnotation.newInstance(typeContext, tableName);
	}

	/**
	 * Creates a new annotation instance to mark a database column.
	 * The annotation is not added to the AST and must be added by the caller;
	 * however any referenced types are added to the imports section of the 
	 * type context.
	 * @param typeContext
	 * @param columnName explicitly specified column name,
	 * null to leave empty and imply the use of a default column name
	 * @return the newly constructed instance
	 */
	public static ParsedAnnotation newDBColumnInstance(ParsedTypeContext typeContext, String columnName) {
		return ParsedDBColumnAnnotation.newInstance(typeContext, columnName);
	}
	
	/**
	 * Creates a new annotation instance to mark a database column
	 * as the primary key.
	 * The annotation is not added to the AST and must be added by the caller;
	 * however any referenced types are added to the imports section of the 
	 * type context.
	 * @param typeContext
	 * @return the newly constructed instance
	 */
	public static ParsedAnnotation newDBPrimaryKeyInstance(ParsedTypeContext typeContext) {
		return ParsedDBPrimaryKeyAnnotation.newInstance(typeContext);
	}

	/**
	 * Creates a new annotation instance to mark a database column
	 * as a foreign key.
	 * The annotation is not added to the AST and must be added by the caller;
	 * however any referenced types are added to the imports section of the 
	 * type context.
	 * 
	 * <p> Note: this method supports foreign keys to non-primary columns
	 * on target tables. At present the actual DBvolution annotation doesn't
	 * support this. It is included here for the rare cases where a database
	 * uses such an FK. It wil cause a compiler error in the generated code
	 * which will allow the end-user to discover the problem easily.
	 * @param typeContext
	 * @param referencedClass class mapped to the referenced table
	 * @param referencedField optional, field maped to the referenced column,
	 *        a {@code null} value implies defaulting to the primary key on the target table
	 * @return the newly constructed instance
	 */
	public static ParsedAnnotation newDBForeignKeyInstance(ParsedTypeContext typeContext,
			ParsedClass referencedClass, ParsedField referencedField) {
		return ParsedDBForeignKeyAnnotation.newInstance(typeContext, referencedClass, referencedField);
	}
	
	public ParsedAnnotation(ParsedTypeContext typeContext, Annotation astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("@").append(getDeclaredTypeName());
		buf.append("(...)");
		return buf.toString();
	}
	
	public Annotation astNode() {
		return astNode;
	}
	
	public ParsedTypeContext typeContext() {
		return typeContext;
	}
	
	/**
	 * Gets the name qualified to the level as it is declared.
	 */
	public String getDeclaredTypeName() {
		return astNode.getTypeName().getFullyQualifiedName();
	}
	
	/**
	 * Gets the simple name.
	 */
	public String getSimpleTypeName() {
		String name = astNode.getTypeName().getFullyQualifiedName();
		if (name.contains(".")) {
			return name.substring(name.lastIndexOf(".")+1);
		}
		return name;
	}
	
	/**
	 * Gets the (inferred) fully qualified name.
	 * This value returned by this method is inferred where possible,
	 * and left as the declared name when not possible due to ambiguous wildcard imports.
	 */
	public String getQualifiedTypeName() {
		return typeContext.getFullyQualifiedNameOf(getDeclaredTypeName());
	}
	
	/**
	 * Indicates whether this annotation is of the specified type.
	 * @param annotationType
	 * @return true if of the specified type, false if not the type or not known
	 */
	public boolean isType(Class<?> annotationType) {
		return typeContext.isDeclarationOfType(annotationType, getDeclaredTypeName());
	}
	
	/**
	 * Gets the Expression AST node for the specified attribute.
	 * Recognises the different types of annotations, and that
	 * {@code "value"} attributes can be specified without name in
	 * single member annotations.
	 * @param attributeName
	 * @return
	 */
	public Expression getAttributeExpressionIfSet(String attributeName) {
		if (attributeName.equals("value") && astNode() instanceof SingleMemberAnnotation) {
			return ((SingleMemberAnnotation) astNode()).getValue();
		}
		else if (astNode() instanceof NormalAnnotation) {
			for (MemberValuePair pair: (List<MemberValuePair>)((NormalAnnotation) astNode()).values()) {
				if (pair.getName().getFullyQualifiedName().equals(attributeName)) {
					return pair.getValue();
				}
			}
		}
		return null; // not found
	}
	
	/**
	 * Gets the string value for the specified attribute.
	 * Recognises the different types of annotations, and that
	 * {@code "value"} attributes can be specified without name in
	 * single member annotations.
	 * 
	 * <p> If the attribute is available, but the wrong type, it throws
	 * an {@link OperationUnsupportedException}.
	 * @param attributeName
	 * @return
	 * @throws OperationUnsupportedException
	 */
	public String getStringAttributeIfSet(String attributeName) {
		Expression expr = getAttributeExpressionIfSet(attributeName);
		
		// FIXME: this needs to be able to handle references to constants
		if (expr != null) {
			if (expr.getNodeType() == ASTNode.STRING_LITERAL) {
				return ((StringLiteral)expr).getLiteralValue();
			}
			else {
				throw new UnsupportedOperationException("Unable to handle @"+getDeclaredTypeName()+
						" with values set via "+expr.getClass().getSimpleName());
			}
		}
		
		return null;
	}

	/**
	 * Gets the type value for the specified attribute.
	 * Recognises the different types of annotations, and that
	 * {@code "value"} attributes can be specified without name in
	 * single member annotations.
	 * 
	 * <p> If the attribute is available, but the wrong type, it throws
	 * an {@link OperationUnsupportedException}.
	 * @param attributeName
	 * @return
	 * @throws OperationUnsupportedException
	 */
	public Type getTypeAttributeIfSet(String attributeName) {
		Expression expr = getAttributeExpressionIfSet(attributeName);
		
		// FIXME: this needs to be able to handle references to constants
		if (expr != null) {
			if (expr.getNodeType() == ASTNode.TYPE_LITERAL) {
				return ((TypeLiteral)expr).getType();
			}
			else {
				throw new UnsupportedOperationException("Unable to handle @"+getDeclaredTypeName()+
						" with values set via "+expr.getClass().getSimpleName());
			}
		}
		
		return null;
	}

	/**
	 * Indicates whether this is a <code>@DBTableName</code> annotation.
	 */
	public boolean isDBTableName() {
		return ParsedDBTableNameAnnotation.isTypeOf(this);
	}
	
	/**
	 * Indicates whether this is a <code>@DBColumn</code> annotation.
	 */
	public boolean isDBColumn() {
		return ParsedDBColumnAnnotation.isTypeOf(this);
	}
	
	/**
	 * Indicates whether this is a <code>@DBPrimaryKey</code> annotation.
	 */
	public boolean isDBPrimaryKey() {
		return ParsedDBPrimaryKeyAnnotation.isTypeOf(this);
	}

	/**
	 * Indicates whether this is a <code>@DBForeignKey</code> annotation.
	 */
	public boolean isDBForeignKey() {
		return ParsedDBForeignKeyAnnotation.isTypeOf(this);
	}
	
	/**
	 * Decorates this annotation as if it's an instance of
	 * <code>@DBTableName</code>, regardless of whether or not it actually
	 * is one.
	 * Use {@link #isDBTableName()} beforehand to test its actual type.
	 * @return
	 */
	public ParsedDBTableNameAnnotation asDBTableName() {
		return new ParsedDBTableNameAnnotation(this);
	}

	/**
	 * Decorates this annotation as if it's an instance of
	 * <code>@DBColumn</code>, regardless of whether or not it actually
	 * is one.
	 * Use {@link #isDBColumn()} beforehand to test its actual type.
	 * @return
	 */
	public ParsedDBColumnAnnotation asDBColumn() {
		return new ParsedDBColumnAnnotation(this);
	}

	/**
	 * Decorates this annotation as if it's an instance of
	 * <code>@DBPrimaryKey</code>, regardless of whether or not it actually
	 * is one.
	 * Use {@link #isDBPrimaryKey()} beforehand to test its actual type.
	 * @return
	 */
	public ParsedDBPrimaryKeyAnnotation asDBPrimaryKey() {
		return new ParsedDBPrimaryKeyAnnotation(this);
	}

	/**
	 * Decorates this annotation as if it's an instance of
	 * <code>@DBForeignKey</code>, regardless of whether or not it actually
	 * is one.
	 * Use {@link #isDBForeignKey()} beforehand to test its actual type.
	 * @return
	 */
	public ParsedDBForeignKeyAnnotation asDBForeignKey() {
		return new ParsedDBForeignKeyAnnotation(this);
	}

	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation,
	 * if set.
	 * @return {@code null} if not applicable
	 * @deprecated to be replaced by a {@code asDBColumn()} or similar method
	 */
	@Deprecated
	public String getColumnNameIfSet() {
		return new ParsedDBColumnAnnotation(this).getColumnNameIfSet();
	}
}
