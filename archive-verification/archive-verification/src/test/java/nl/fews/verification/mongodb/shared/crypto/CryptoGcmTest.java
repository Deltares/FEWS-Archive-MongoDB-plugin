package nl.fews.verification.mongodb.shared.crypto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CryptoGcmTest {

	@Test
	void encrypt() {
		assertNotEquals("message", CryptoGcm.encrypt("message."));
	}

	@Test
	void decrypt() {
		String cipherText = CryptoGcm.encrypt("message");
        assertEquals("message", CryptoGcm.decrypt(cipherText));
	}

	@Test
	void getPassword() {
		System.out.println(CryptoGcm.encrypt("Test"));
	}

	@Test
	void generateKey() {
		System.out.println(CryptoGcm.generateKey());
	}
}