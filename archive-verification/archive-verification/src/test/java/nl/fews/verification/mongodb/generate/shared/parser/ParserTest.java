package nl.fews.verification.mongodb.generate.shared.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParserTest {

	@Test
	void parseFloat() {
        assertEquals(1.5f, Parser.parseFloat("1.5"));

		assertEquals("abc", Parser.parseFloat("abc"));

		assertEquals("1.5abc", Parser.parseFloat("1.5abc"));

		Object input = new Object();
        assertSame(input, Parser.parseFloat(input));
	}
}