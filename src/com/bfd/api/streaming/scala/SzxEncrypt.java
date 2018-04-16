package com.bfd.api.streaming.scala;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * 加密解密类
 * @author yuhuan.li
 */
public class SzxEncrypt {
	public static final String KEY = "1234567887654321";
	public static final String IV = "8765432112345678";
	/**
	 * 加密
	 * @param str 明文
	 * @return 密文
	 */
	public static String encrypt(String str) {
		try{
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
			int blockSize = cipher.getBlockSize();
			byte[] dataBytes = str.getBytes();
			int plaintextLength = dataBytes.length;
			if (plaintextLength % blockSize != 0) {
				plaintextLength = plaintextLength + (blockSize - (plaintextLength % blockSize));
			}
			byte[] plaintext = new byte[plaintextLength];
			System.arraycopy(dataBytes, 0, plaintext, 0, dataBytes.length);
			SecretKeySpec keyspec = new SecretKeySpec(KEY.getBytes(), "AES");
			IvParameterSpec ivspec = new IvParameterSpec(IV.getBytes());
			cipher.init(Cipher.ENCRYPT_MODE, keyspec, ivspec);
			byte[] encrypted = cipher.doFinal(plaintext);
			return new sun.misc.BASE64Encoder().encode(encrypted);
		} catch (Exception e) {
			System.out.println("加密错误" + e.getMessage());
			return null;
		}
	}

	/**
	 * 解密
	 * @param str 密文
	 * @return 明文
	 */
	public static String decrypt(String str) {
		try {
			byte[] encrypted1 = new sun.misc.BASE64Decoder().decodeBuffer(str);
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
			SecretKeySpec keyspec = new SecretKeySpec(KEY.getBytes(), "AES");
			IvParameterSpec ivspec = new IvParameterSpec(IV.getBytes());
			cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
			byte[] original = cipher.doFinal(encrypted1);
			String originalString = new String(original);
			return originalString.trim();
		} catch (Exception e) {
			System.out.println("解密错误" + e.getMessage());
			return null;
		}
	}

	public static void main(String[] args) {
		args = new String[2];
		args[0]="decrypt";
		args[1]="Zmo4sq3vQISb9UlPD8dk5w==";
		if(args.length!=2){
			System.out.println("参数必须是2个,且必须是encrypt空格明文或者是decrypt空格密文");
			return;
		}
		if(!"encrypt".equals(args[0])&&!"decrypt".equals(args[0])){
			System.out.println("第一个参数必须是encrypt或者是decrypt");
			return;
		}
		if("encrypt".equals(args[0])){
			String ciphertext1 = encrypt(args[1]);
			System.out.println(ciphertext1);
			return;
		}
		if("decrypt".equals(args[0])){
			String text1 = decrypt(args[1]);
			System.out.println(text1);
			return;
		}
		
		
	}
}