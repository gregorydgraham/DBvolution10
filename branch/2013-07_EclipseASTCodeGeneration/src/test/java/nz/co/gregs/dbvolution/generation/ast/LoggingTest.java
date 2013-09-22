package nz.co.gregs.dbvolution.generation.ast;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingTest {
	private static final Logger log = LoggerFactory.getLogger(LoggingTest.class);
	
	@Test
	public void aoeu() {
		log.info("bla blah");
	}
}
