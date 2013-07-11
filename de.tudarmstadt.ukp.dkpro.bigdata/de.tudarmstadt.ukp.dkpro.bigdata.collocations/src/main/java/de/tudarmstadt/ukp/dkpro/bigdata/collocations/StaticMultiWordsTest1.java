package de.tudarmstadt.ukp.dkpro.bigdata.collocations;

import static org.junit.Assert.*;

import org.junit.Test;

public class StaticMultiWordsTest1 {

	@Test
	public void testMatches() {
		assertEquals("iwo jima",StaticMultiWords.matches(new String[]{"iwo","jima"}));
		assertEquals("iwo jima",StaticMultiWords.matches(new String[]{"in","iwo","jima"}));
		assertEquals("iwo jima",StaticMultiWords.matches(new String[]{null,"iwo","jima"}));
		assertEquals("out of control",StaticMultiWords.matches(new String[]{"out","of","control"}));
		assertEquals(null,StaticMultiWords.matches(new String[]{"iwo","jimd"}));
		assertEquals(null,StaticMultiWords.matches(new String[]{null,null,"jima"}));
		assertEquals(null,StaticMultiWords.matches(new String[]{null,"hallo","jima"}));

	}

}
