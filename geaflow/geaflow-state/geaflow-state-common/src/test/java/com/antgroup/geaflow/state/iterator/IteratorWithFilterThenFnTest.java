package com.antgroup.geaflow.state.iterator;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IteratorWithFilterThenFnTest {

    @Test
    public void testFunction() {
        IteratorWithFilterThenFn<Integer, String> it =
            new IteratorWithFilterThenFn<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o > 3, Object::toString);

        ArrayList<String> list = Lists.newArrayList(it);
        Assert.assertEquals(list.size(), 2);
        Assert.assertTrue(list.contains("4"));
        Assert.assertTrue(list.contains("5"));

        it = new IteratorWithFilterThenFn<>(Arrays.asList(1, 2, 3, 4, 5).iterator(),
            o -> o <= 3, Objects::toString);
        list = Lists.newArrayList(it);
        Assert.assertEquals(list.size(), 3);
    }
}