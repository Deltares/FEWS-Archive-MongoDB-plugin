package nl.fews.verification.mongodb.shared.mail;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MailTest {

	@Test
	void send() {
		Mail.send("Test", "Me");
	}
}