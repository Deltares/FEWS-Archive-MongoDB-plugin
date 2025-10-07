package nl.fews.verification.mongodb.shared.crypto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CryptoOfbTest {

	@Test
	void encrypt() {
		assertNotEquals("message", CryptoOfb.encrypt("message."));
	}

	@Test
	void decrypt() {
		String cipherText = CryptoOfb.encrypt("message");
        assertEquals("message", CryptoOfb.decrypt(cipherText));
	}

	@Test
	void getPassword() {
		System.out.println(CryptoOfb.encrypt("Test"));
	}

	@Test
	void generateKey() {
		System.out.println(CryptoOfb.generateKey());
	}
}