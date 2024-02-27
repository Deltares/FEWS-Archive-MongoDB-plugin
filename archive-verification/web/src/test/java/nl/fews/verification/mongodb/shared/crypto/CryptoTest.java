package nl.fews.verification.mongodb.shared.crypto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CryptoTest {

	@Test
	void encrypt() {
		assertNotEquals("message", Crypto.encrypt("message."));
	}

	@Test
	void decrypt() {
		String cipherText = Crypto.encrypt("message");
        assertEquals("message", Crypto.decrypt(cipherText));
	}

	@Test
	void getPassword() {
		System.out.println(Crypto.encrypt("Test"));
	}

	@Test
	void generateKey() {
		System.out.println(Crypto.generateKey());
	}

	@Test
	void generateIv() {
		System.out.println(Crypto.generateIv());
	}
}