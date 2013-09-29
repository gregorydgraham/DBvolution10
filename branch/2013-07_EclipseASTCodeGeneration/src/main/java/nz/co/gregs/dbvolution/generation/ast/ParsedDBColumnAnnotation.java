package nz.co.gregs.dbvolution.generation.ast;

import nz.co.gregs.dbvolution.annotations.DBColumn;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;

/**
 * Decorates annotations as <code>@DBColumn</code> annotations.
 * Also provides factory methods for creating instances of the
 * annotation itself.
 */
public class ParsedDBColumnAnnotation extends ParsedAnnotationWrapper {

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
	public static ParsedAnnotation newInstance(ParsedTypeContext typeContext, String columnName) {
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
	 * Indicates whether the supplied annotation is of this type
	 * @param annotation
	 */
	public static boolean isTypeOf(ParsedAnnotation annotation) {
		return annotation.isType(DBColumn.class);
	}
	
	/**
	 * Wraps the given annotation
	 * @param annotation
	 */
	public ParsedDBColumnAnnotation(ParsedAnnotation annotation) {
		super(annotation);
	}
	
	/**
	 * Gets the column name, as specified via the annotation, if set.
	 * Does not apply any defaulting mechanism.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		if (!isTypeOf(delegate)) {
			return null;
		}
		return delegate.getStringAttributeIfSet("value");
	}

}
