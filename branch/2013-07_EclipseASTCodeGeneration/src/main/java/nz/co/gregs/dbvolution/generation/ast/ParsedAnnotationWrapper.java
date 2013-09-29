package nz.co.gregs.dbvolution.generation.ast;

import org.eclipse.jdt.core.dom.Annotation;

/**
 * Decorates a {@link ParsedAnnotation} with features suitable for treating
 * it as a particular type of annotation.
 * Implementations should behave correctly regardless of whether the annotation
 * actually is of the expected type: it should just return nulls etc. 
 */
abstract class ParsedAnnotationWrapper {
	protected final ParsedAnnotation delegate;

	ParsedAnnotationWrapper(ParsedAnnotation delegate) {
		this.delegate = delegate;
	}
	
	/**
	 * @return
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return delegate.hashCode();
	}

	/**
	 * @param obj
	 * @return
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		return delegate.equals(obj);
	}

	/**
	 * @return
	 * @see nz.co.gregs.dbvolution.generation.ast.ParsedAnnotation#toString()
	 */
	public String toString() {
		return delegate.toString();
	}

	/**
	 * @return
	 * @see nz.co.gregs.dbvolution.generation.ast.ParsedAnnotation#astNode()
	 */
	public Annotation astNode() {
		return delegate.astNode();
	}

	/**
	 * @return
	 * @see nz.co.gregs.dbvolution.generation.ast.ParsedAnnotation#getDeclaredTypeName()
	 */
	public String getDeclaredTypeName() {
		return delegate.getDeclaredTypeName();
	}

	/**
	 * @return
	 * @see nz.co.gregs.dbvolution.generation.ast.ParsedAnnotation#getSimpleTypeName()
	 */
	public String getSimpleTypeName() {
		return delegate.getSimpleTypeName();
	}

	/**
	 * @return
	 * @see nz.co.gregs.dbvolution.generation.ast.ParsedAnnotation#getQualifiedTypeName()
	 */
	public String getQualifiedTypeName() {
		return delegate.getQualifiedTypeName();
	}
}
