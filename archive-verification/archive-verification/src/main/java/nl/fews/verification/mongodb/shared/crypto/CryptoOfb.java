package nl.fews.verification.mongodb.shared.crypto;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

@SuppressWarnings({"unused"})
public final class CryptoOfb {
	private static final String KEY = "uzyHy/GhHIfSr/uWaVWl2gqnoudfZ4rbuAxlmib54DQ=";

	private CryptoOfb(){}

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
	 * Encrypts the given plain text using AES encryption algorithm with OFB mode and no padding.
	 *
	 * @param plainText The plain text to be encrypted.
	 * @return The encrypted cipher text as a Base64 encoded string.
	 * @throws RuntimeException If an error occurs during encryption.
	 */
	public static String encrypt(String plainText) {
		try {
			Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");
			byte[] nonce = new byte[128/8];
			new SecureRandom().nextBytes(nonce);
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(Base64.getDecoder().decode(KEY), "AES"), new IvParameterSpec(nonce));
			return String.format("%s|%s", Base64.getEncoder().encodeToString(nonce), Base64.getEncoder().encodeToString(cipher.doFinal(plainText.getBytes())));
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
			String[] parts = cipherText.split("\\|");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(Base64.getDecoder().decode(KEY), "AES"), new IvParameterSpec(Base64.getDecoder().decode(parts[0])));
			return new String(cipher.doFinal(Base64.getDecoder().decode(parts[1])));
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
