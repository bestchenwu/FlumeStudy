package com.babytree.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 通用的日期工具类
 *
 * @author chenwu on 2020.10.27
 */
public class CommonDateUtil {

    /**
     * 获取当前时间的年-月-日 小时:分钟:秒的字符串表示方法
     *
     * @return {@link String}
     * @author chenwu on 2020.10.27
     */
    public static String getCurrentTime(String timePattern){
        LocalDateTime localDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
        String dateFormatStr = localDateTime.format(formatter);
        return dateFormatStr;
    }
}
