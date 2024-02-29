package nl.fews.verification.mongodb.shared.crypto;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

@SuppressWarnings({"unused"})
public final class Crypto {
	private static final String KEY = "uzyHy/GhHIfSr/uWaVWl2gqnoudfZ4rbuAxlmib54DQ=";
    private static final String IV = "fi5iKoY0nj1fsKa+yaGn3A==";

	private Crypto(){}

	/**
	 *
	 * Generates a random encryption key for use in AES encryption algorithm.
	 *
	 * @return The generated encryption key as a Base64 encoded string.
	 * @throws RuntimeException If an error occurs during key generation.
	 */
	public static String generateKey() {
		try {
			KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
			keyGenerator.init(256);
			return Base64.getEncoder().encodeToString(keyGenerator.generateKey().getEncoded());
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Generates a random Initialization Vector (IV) for use in encryption algorithms.
	 *
	 * @return The generated IV as a Base64 encoded string.
	 */
	public static String generateIv() {
		byte[] iv = new byte[128/8];
		new SecureRandom().nextBytes(iv);
		return Base64.getEncoder().encodeToString(iv);
	}

	/**
	 * Encrypts the given plain text using AES encryption algorithm with OFB mode and no padding.
	 *
	 * @param plainText The plain text to be encrypted.
	 * @return The encrypted cipher text as a Base64 encoded string.
	 * @throws RuntimeException If an error occurs during encryption.
	 */
	public static String encrypt(String plainText) {
		try {
			Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(Base64.getDecoder().decode(KEY), "AES"), new IvParameterSpec(Base64.getDecoder().decode(IV)));
			return Base64.getEncoder().encodeToString(cipher.doFinal(plainText.getBytes()));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Decrypts the given cipher text using the AES/OFB/NoPadding encryption algorithm.
	 *
	 * @param cipherText The cipher text to decrypt.
	 * @return The decrypted plain text.
	 * @throws RuntimeException If an error occurs during decryption.
	 */
	public static String decrypt(String cipherText) {
		try {
			Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(Base64.getDecoder().decode(KEY), "AES"), new IvParameterSpec(Base64.getDecoder().decode(IV)));
			return new String(cipher.doFinal(Base64.getDecoder().decode(cipherText)));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
