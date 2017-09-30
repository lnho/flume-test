package org.apache.flume.sink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Cal {
    public static void main(String[] args) throws ParseException {
        String startTime = "15:23:27,785";
        long startEventNum = 10000;
        String endTime = "15:34:04,594";
        long endEventNum = 770000+650000+600000+780000+750000+600000+720000+740000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss,SSS");
        Date startDate = simpleDateFormat.parse(startTime);
        Date endDate = simpleDateFormat.parse(endTime);
        int fileLines = 1000;
        double res = (endEventNum - startEventNum) * 560_090000.0 / fileLines / 1024 / 1024 / (endDate.getTime() - startDate.getTime());
        System.out.printf("吞吐为：%.2fMB/s", res);
    }
}
