package com.babytree.util;

import org.junit.Test;

public class CommonDateUtilTest {

    @Test
    public void testGetCurrentTime(){
        String currentTime = CommonDateUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss");
        System.out.println(currentTime);
    }
}
