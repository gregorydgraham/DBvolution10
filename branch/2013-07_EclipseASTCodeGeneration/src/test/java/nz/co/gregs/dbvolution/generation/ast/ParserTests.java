package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty;
import nz.co.gregs.dbvolution.generation.ast.ParsedClass;

import org.eclipse.jface.text.BadLocationException;
import org.eclipse.text.edits.MalformedTreeException;
import org.junit.Test;

public class ParserTests extends AbstractASTTest {
	@Test
	public void getProperties() throws MalformedTreeException, BadLocationException {
		ParsedClass javatype = ParsedClass.parseContents(getMarqueSource());
		
		List<ParsedBeanProperty> properties = javatype.getDBColumnProperties();
		for (ParsedBeanProperty property: properties) {
			System.out.println("---- Property: "+property.getType().toString()+" "+
					property.getName()+
					" (Column '"+property.getColumnNameIfSet()+
					"') ----");
			System.out.println(property);
			System.out.println();
		}
	}
}
