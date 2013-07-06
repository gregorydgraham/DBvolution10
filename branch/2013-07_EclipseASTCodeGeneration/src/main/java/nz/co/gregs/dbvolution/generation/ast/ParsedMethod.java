package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.MethodDeclaration;

public class ParsedMethod {
	private ParsedTypeContext typeContext;
	private MethodDeclaration astNode;
	private List<ParsedAnnotation> annotations;
	
	public ParsedMethod(ParsedTypeContext typeContext, MethodDeclaration astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
		
    	// method annotations
		this.annotations = new ArrayList<ParsedAnnotation>();
    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
    		if (modifier.isAnnotation()) {
    			annotations.add(new ParsedAnnotation(typeContext, (Annotation)modifier));
    		}
    	}
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (ParsedAnnotation annotation: annotations) {
			buf.append(annotation).append("\n");
		}
		buf.append("method "+getName());
		buf.append("();");
		return buf.toString();
	}
	
	public String getName() {
		return astNode.getName().getFullyQualifiedName();
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return annotations;
	}
}
