package nz.co.gregs.dbvolution.generation.ast;

import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;

/**
 * Decorates annotations as <code>@DBTable</code> annotations.
 * Also provides factory methods for creating instances of the
 * annotation itself.
 */
public class ParsedDBTableNameAnnotation extends ParsedAnnotationWrapper {

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
	public static ParsedAnnotation newInstance(ParsedTypeContext typeContext, String tableName) {
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
	 * Indicates whether the supplied annotation is of this type
	 * @param annotation
	 */
	public static boolean isTypeOf(ParsedAnnotation annotation) {
		return annotation.isType(DBTableName.class);
	}
	
	/**
	 * Wraps the given annotation
	 * @param annotation
	 */
	public ParsedDBTableNameAnnotation(ParsedAnnotation annotation) {
		super(annotation);
	}
	
	/**
	 * Gets the table name, as specified via the annotation, if set.
	 * Does not apply any defaulting mechanism.
	 * @return {@code null} if not applicable
	 */
	public String getTableNameIfSet() {
		if (!isTypeOf(delegate)) {
			return null;
		}
		return delegate.getStringAttributeIfSet("value");
	}

}
