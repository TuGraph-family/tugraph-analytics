package com.antgroup.geaflow.dsl.udf.string;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.udf.table.string.LTrim;
import com.antgroup.geaflow.dsl.udf.table.string.RTrim;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TrimTest {

    @Test
    public void testLTrim() {
        LTrim lTrim = new LTrim();
        Assert.assertEquals(lTrim.eval(BinaryString.fromString(" abc")), BinaryString.fromString("abc"));
        Assert.assertEquals(lTrim.eval(BinaryString.fromString("abc")), BinaryString.fromString("abc"));
        Assert.assertEquals(lTrim.eval(BinaryString.fromString("  abc")), BinaryString.fromString("abc"));
        Assert.assertEquals(lTrim.eval(BinaryString.fromString("  ")), BinaryString.fromString(""));
    }

    @Test
    public void testRLTrim() {
        RTrim rTrim = new RTrim();
        Assert.assertEquals(rTrim.eval(BinaryString.fromString(" abc ")), BinaryString.fromString(" abc"));
        Assert.assertEquals(rTrim.eval(BinaryString.fromString("abc  ")), BinaryString.fromString("abc"));
        Assert.assertEquals(rTrim.eval(BinaryString.fromString("  ")), BinaryString.fromString(""));
    }
}
