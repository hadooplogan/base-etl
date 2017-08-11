/**
 * 
 */
package com.chinadaas.common.common;

import java.util.Enumeration;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * 
 */
public class CommonConfig {
	private static final String BUNDLE_NAME = "common-config";
	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	public static String getValue(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key).trim();
		} catch (MissingResourceException e) {
			return "";
		}
	}
	public static Enumeration<String> getKeys() {
	    return RESOURCE_BUNDLE.getKeys();
	}
}
