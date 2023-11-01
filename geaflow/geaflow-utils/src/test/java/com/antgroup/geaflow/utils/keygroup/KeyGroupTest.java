package com.antgroup.geaflow.utils.keygroup;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyGroupTest {

    @Test
    public void testKeyGroupContains() {
        KeyGroup a = new KeyGroup(0, 100);
        KeyGroup b = new KeyGroup(0, 1);
        Assert.assertTrue(a.contains(b));

        KeyGroup c = new KeyGroup(0, 100);
        Assert.assertTrue(a.contains(c));

        KeyGroup d = new KeyGroup(9, 101);
        Assert.assertFalse(a.contains(d));
    }

}