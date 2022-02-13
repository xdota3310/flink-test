package com.lr;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        BloomFilter<CharSequence> charSequenceBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 1000000, 0.04);
        System.out.println(charSequenceBloomFilter.put("1"));
        System.out.println(charSequenceBloomFilter.put("11"));
        System.out.println(charSequenceBloomFilter.put("111"));
        System.out.println(charSequenceBloomFilter.put("1111"));
        System.out.println(charSequenceBloomFilter.put("11111"));
        System.out.println(charSequenceBloomFilter.put("111111"));
        assertTrue(true);
    }
}
