package com.chinadaas.association.util;

import java.util.UUID;

public class IDUtil {
	public static UUID getUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid;
	}

	public static String getUserId(String value) {
		return MD5Util.getMd5(value);
	}
}
