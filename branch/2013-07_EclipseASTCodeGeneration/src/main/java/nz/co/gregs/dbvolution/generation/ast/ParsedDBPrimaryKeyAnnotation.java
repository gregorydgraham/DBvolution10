package nz.co.gregs.dbvolution.generation.ast;

import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.MarkerAnnotation;

/**
 * Decorates annotations as <code>@DBPrimaryKey</code> annotations.
 * Also provides factory methods for creating instances of the
 * annotation itself.
 */
public class ParsedDBPrimaryKeyAnnotation extends ParsedAnnotationWrapper {

	/**
	 * Creates a new annotation instance to mark a database column
	 * as the primary key.
	 * The annotation is not added to the AST and must be added by the caller;
	 * however any referenced types are added to the imports section of the 
	 * type context.
	 * @param typeContext
	 * @return the newly constructed instance
	 */
	public static ParsedAnnotation newInstance(ParsedTypeContext typeContext) {
		AST ast = typeContext.getAST();
		
		MarkerAnnotation annotation = ast.newMarkerAnnotation();
		annotation.setTypeName(typeContext.declarableTypeNameOf(DBPrimaryKey.class, true));
		
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
	public ParsedDBPrimaryKeyAnnotation(ParsedAnnotation annotation) {
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
