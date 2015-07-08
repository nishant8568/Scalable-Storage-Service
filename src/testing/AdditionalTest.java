package testing;

import junit.framework.TestCase;

import org.junit.Test;

import client.KVStore;

import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	KVStore kvClient = new KVStore("localhost", 10000);
	@Test
	public void testStub() {
		assertTrue(true);
	}
	
	@Test
	/**
	 * To test the validity of key. Maximum key size allowed is 20B
	 */
	public void testValidKey()
	{
		String key = "Checking keyLength Case";
		String value = "bar";
		TextMessage response = null;
		Exception ex = null;
		try {
			response = (TextMessage)kvClient.put(key, value);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			ex = e1;
		}
		assertFalse("Key Length exceeded the maximum size limit of 20 Bytes",ex == null
				&& response.getStatus() == StatusType.PUT_ERROR && response.getComments().equals("INVALID_KEY"));
	}
	
	@Test
	/**
	 * To test the validity of value. Maximum key size allowed is 120 kB 
	 */
	public void testValidValue()
	{
		String key = "foo";
		String value = "bar";
		TextMessage response = null;
		Exception ex = null;
		try {
			response = (TextMessage)kvClient.put(key, value);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
		}
		assertFalse("Value Length exceeded the maximum size limit of 120 kiloBytes",ex == null 
				&& response.getStatus() == StatusType.PUT_ERROR && response.getComments().equals("INVALID_VALUE"));
	}
	
	@Test
	/**
	 * To test the deletion functionality when key is not present in the storage server
	 */
	public void deletionValidity()
	{
		String key = "foo";
		String value = "null";
		TextMessage response = null;
		Exception ex = null;
		try {
			response = (TextMessage)kvClient.put(key, value);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
		}
		assertFalse("Deletion failed as key does not exist",ex == null 
				&& response.getStatus() == StatusType.PUT_ERROR && response.getComments().equals("KEY_ABSENT"));
	}
	
	
	
	
}
