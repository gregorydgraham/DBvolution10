package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.Expression;
import org.eclipse.jdt.core.dom.MarkerAnnotation;
import org.eclipse.jdt.core.dom.MemberValuePair;
import org.eclipse.jdt.core.dom.NormalAnnotation;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;
import org.eclipse.jdt.core.dom.TypeLiteral;

/**
 * Represents an annotation in a source file associated with any
 * source file component.
 */
public class ParsedAnnotation {
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
		AST ast = typeContext.getAST();

		SingleMemberAnnotation annotation = ast.newSingleMemberAnnotation();
		annotation.setTypeName(typeContext.declarableTypeNameOf(DBTableName.class, true));
		
		if (tableName != null) {
			StringLiteral annotationValue = ast.newStringLiteral();
			annotationValue.setLiteralValue(tableName);
			annotation.setValue(annotationValue);
		}

		return new ParsedAnnotation(typeContext, annotation);
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
		AST ast = typeContext.getAST();
		
		SingleMemberAnnotation annotation = ast.newSingleMemberAnnotation();
		annotation.setTypeName(typeContext.declarableTypeNameOf(DBColumn.class, true));
		
		if (columnName != null) {
			StringLiteral annotationValue = ast.newStringLiteral();
			annotationValue.setLiteralValue(columnName);
			annotation.setValue(annotationValue);
		}
		
		return new ParsedAnnotation(typeContext, annotation);
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
		AST ast = typeContext.getAST();
		
		MarkerAnnotation annotation = ast.newMarkerAnnotation();
		annotation.setTypeName(typeContext.declarableTypeNameOf(DBPrimaryKey.class, true));
		
		return new ParsedAnnotation(typeContext, annotation);
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
		AST ast = typeContext.getAST();
		Annotation annotation;
		
		// annotation with default value only
		if (referencedField == null) {
			SingleMemberAnnotation smAnnotation = ast.newSingleMemberAnnotation();
			smAnnotation.setTypeName(typeContext.declarableTypeNameOf(DBForeignKey.class, true));
			annotation = smAnnotation;

			TypeLiteral value = ast.newTypeLiteral();
			value.setType(ast.newSimpleType(ast.newSimpleName(referencedClass.getDeclaredName())));
			smAnnotation.setValue(value);
		}
		
		// annotation with two values
		else {
			NormalAnnotation normalAnnotation = ast.newNormalAnnotation();
			normalAnnotation.setTypeName(typeContext.declarableTypeNameOf(DBForeignKey.class, true));
			annotation = normalAnnotation;
			
			MemberValuePair pair1 = ast.newMemberValuePair();
			TypeLiteral value1 = ast.newTypeLiteral();
			value1.setType(ast.newSimpleType(ast.newSimpleName(referencedClass.getDeclaredName())));
			pair1.setName(ast.newSimpleName("value"));
			pair1.setValue(value1);
			
			MemberValuePair pair2 = ast.newMemberValuePair();
			StringLiteral value2 = ast.newStringLiteral();
			value2.setEscapedValue(referencedField.getName());
			pair2.setName(ast.newSimpleName("value"));
			pair2.setValue(value2);
			
			normalAnnotation.values().add(pair1);
			normalAnnotation.values().add(pair2);
		}
		
		return new ParsedAnnotation(typeContext, annotation);
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
	 * Indicates whether this annotation is {@link nz.co.gregs.dbvolution.annotations.DBTableColumn}.
	 */
	public boolean isDBColumn() {
		return typeContext.isDeclarationOfType(DBColumn.class, getDeclaredTypeName());
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation,
	 * if set.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		if (!isDBColumn()) {
			return null;
		}
		
		Expression expr = null;
		if (astNode instanceof SingleMemberAnnotation) {
			expr = ((SingleMemberAnnotation) astNode).getValue();
		}
		else if (astNode instanceof NormalAnnotation) {
			for (MemberValuePair pair: (List<MemberValuePair>)((NormalAnnotation) astNode).values()) {
				if (pair.getName().getFullyQualifiedName().equals("value")) {
					expr = pair.getValue();
					break;
				}
			}
		}

		if (expr != null) {
			if (expr.getNodeType() == ASTNode.STRING_LITERAL) {
				return ((StringLiteral)expr).getLiteralValue();
			}
			else {
				throw new UnsupportedOperationException("Unable to handle @DBTableColumn with values set via "+expr.getClass().getSimpleName());
			}
		}
		return null;
	}
}
