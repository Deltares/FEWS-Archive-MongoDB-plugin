package nl.fews.verification.mongodb.shared.parser;

import nl.fews.verification.mongodb.generate.shared.parser.Parser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParserTest {

	@Test
	void parseFloat() {
        Assertions.assertEquals(1.5f, Parser.parseFloat("1.5"));

		assertEquals("abc", Parser.parseFloat("abc"));

		assertEquals("1.5abc", Parser.parseFloat("1.5abc"));

		Object input = new Object();
        assertSame(input, Parser.parseFloat(input));
	}
}