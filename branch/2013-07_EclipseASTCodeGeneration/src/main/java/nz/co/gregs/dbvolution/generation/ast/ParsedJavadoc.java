package nz.co.gregs.dbvolution.generation.ast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.Javadoc;
import org.eclipse.jdt.core.dom.TagElement;
import org.eclipse.jdt.core.dom.TextElement;

/**
 * Currently no support for actually working with parsed javadoc,
 * but contains convenience methods for generating javadoc AST nodes.
 */
public class ParsedJavadoc {
	/**
	 * Generates a javadoc instance suitable for associating with a class.
	 * @param typeContext
	 * @param text multi-line description of class or null
	 * @return
	 */
	public static Javadoc astClassInstance(ParsedTypeContext typeContext, String text) {
		AST ast = typeContext.getAST();

		Javadoc javadoc = ast.newJavadoc();
		for (String line: linesOf(text)) {
			TextElement javadocText = ast.newTextElement();
			javadocText.setText(line);
			TagElement javadocTag = ast.newTagElement();
			javadocTag.fragments().add(javadocText);
			
			javadoc.tags().add(javadocTag);
		}
		
		return javadoc;
	}
	
	/**
	 * Generates a javadoc instance suitable for associating with a method.
	 * @param typeContext
	 * @param text multi-line description of method or null
	 * @param parameters parameter descriptions array of {@code String[2]} where
	 * {@code String[0]} is the param name and {@code String[1]} is the param description.
	 * Null for no parameters.
	 * @param returnDescription description of return tag, or null for no tag
	 * @return
	 */
	public static Javadoc astMethodInstance(ParsedTypeContext typeContext, String text,
			String[][] parameters, String returnDescription) {
		AST ast = typeContext.getAST();
		
		Javadoc javadoc = ast.newJavadoc();
		
		// main text
		for (String line: linesOf(text)) {
			TextElement javadocText = ast.newTextElement();
			javadocText.setText(line);
			TagElement javadocTag = ast.newTagElement();
			javadocTag.fragments().add(javadocText);
			
			javadoc.tags().add(javadocTag);
		}
		
		// parameters
		if (parameters != null) {
			for (String[] param: parameters) {
				if (param.length != 2) {
					throw new IllegalArgumentException("Invalid parameter array "+Arrays.toString(param)+
							", expected length 2, encountered length "+param.length);
				}
				String paramName = param[0];
				String paramDescription = param[1];
				
				TextElement javadocParamDescriptionText = ast.newTextElement();
				javadocParamDescriptionText.setText(paramDescription);
				
				TagElement javadocTag = ast.newTagElement();
				javadocTag.setTagName("@param");
				javadocTag.fragments().add(ast.newSimpleName(paramName));
				javadocTag.fragments().add(javadocParamDescriptionText);
				
				javadoc.tags().add(javadocTag);
			}
		}
		
		// return
		if (returnDescription != null) {
			TextElement javadocText = ast.newTextElement();
			javadocText.setText(returnDescription);
			TagElement javadocTag = ast.newTagElement();
			javadocTag.setTagName("@return");
			javadocTag.fragments().add(javadocText);
			javadoc.tags().add(javadocTag);
		}
		
		return javadoc;
	}
	
	private static List<String> linesOf(String text) {
		List<String> lines = new ArrayList<String>();
		if (text != null) {
			try {
				BufferedReader reader = new BufferedReader(new StringReader(text));
				String line;
				while ((line = reader.readLine()) != null) {
					lines.add(line);
				}
			} catch (IOException unexpectedEx) {
				throw new RuntimeException(unexpectedEx);
			}
		}
		return lines;
	}
}
