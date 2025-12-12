/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


package drugiProjekt.network;

import java.util.Random;

/**
 *
 * @author Aleksandar
 */
public class EmulatedSystemClock {

    private long startTime;
    private double jitter;
    //jitter per second,  percentage of deviation per 1 second
    private long offset = 0;

    public EmulatedSystemClock() {
        startTime = System.currentTimeMillis();
        Random r = new Random();
        jitter = (r.nextInt(20 )) / 100d; //divide by 10 to get the interval between [0, 20], and then divide by 100 to get percentage
    }

    public synchronized long currentTimeMillis() {
        long current = System.currentTimeMillis();
        long diff =current - startTime;
        double coef = diff / 1000;
        //dodan offset
        return startTime + Math.round(diff * Math.pow((1+jitter), coef)) +  offset;
    }

    public synchronized long syncWith(Long otherTimestamp) {
        long local = currentTimeMillis();
        if (otherTimestamp != null && otherTimestamp > local) {
            offset += (otherTimestamp - local);
            local = otherTimestamp;
        }
        return local;
    }


}