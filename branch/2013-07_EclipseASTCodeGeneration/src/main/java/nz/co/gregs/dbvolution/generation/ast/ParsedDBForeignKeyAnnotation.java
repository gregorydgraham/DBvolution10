package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBForeignKey;

import org.eclipse.jdt.core.dom.AST;
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
 * Decorates annotations as <code>@DBForeignKey</code> annotations.
 * Also provides factory methods for creating instances of the
 * annotation itself.
 */
public class ParsedDBForeignKeyAnnotation extends ParsedAnnotationWrapper {

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
	public static ParsedAnnotation newInstance(ParsedTypeContext typeContext,
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
			String columnName = referencedField.getColumnNameIfSet();
			if (columnName == null) {
				throw new IllegalArgumentException("ReferencedField doesn't indicate its column name: "+referencedField);
			}
			
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
			value2.setEscapedValue(columnName);
			pair2.setName(ast.newSimpleName("column"));
			pair2.setValue(value2);
			
			normalAnnotation.values().add(pair1);
			normalAnnotation.values().add(pair2);
		}
		
		return new ParsedAnnotation(typeContext, annotation);
	}
	
	/**
	 * Indicates whether the supplied annotation is of this type
	 * @param annotation
	 */
	public static boolean isTypeOf(ParsedAnnotation annotation) {
		return annotation.isType(DBForeignKey.class);
	}
	
	/**
	 * Wraps the given annotation
	 * @param annotation
	 */
	public ParsedDBForeignKeyAnnotation(ParsedAnnotation annotation) {
		super(annotation);
	}
	
	/**
	 * Gets the referenced class name, as specified via the annotation, if set.
	 * @return {@code null} if not applicable
	 */
	public ParsedTypeRef getReferencedClassIfSet() {
		if (!isTypeOf(delegate)) {
			return null;
		}
		
		Type type = delegate.getTypeAttributeIfSet("value");
		if (type != null) {
			return new ParsedTypeRef(delegate.typeContext(), type);
		}
		return null;
	}

	/**
	 * Gets the referenced column name, as specified via the annotation, if set.
	 * @return {@code null} if not applicable
	 */
	public String getReferencedColumnNameIfSet() {
		if (!isTypeOf(delegate)) {
			return null;
		}
		return delegate.getStringAttributeIfSet("column");
	}
}
