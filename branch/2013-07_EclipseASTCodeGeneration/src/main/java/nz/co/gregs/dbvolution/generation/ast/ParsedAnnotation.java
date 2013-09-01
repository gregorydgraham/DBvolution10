package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;

import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.Expression;
import org.eclipse.jdt.core.dom.MemberValuePair;
import org.eclipse.jdt.core.dom.NormalAnnotation;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;

public class ParsedAnnotation {
	private ParsedTypeContext typeContext;
	private Annotation astNode;
	
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
	public boolean isDBTableColumn() {
		return typeContext.isDeclarationOfType(DBColumn.class, getDeclaredTypeName());
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation,
	 * if set.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		if (!isDBTableColumn()) {
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
