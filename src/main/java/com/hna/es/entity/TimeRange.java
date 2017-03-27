package com.hna.es.entity;

import com.hna.es.util.DateUtil;

import java.text.ParseException;
import java.util.Date;

/**
 * Represents a time range between two time point.
 */
public class TimeRange {
    private Date starting;
    private Date ending;
    private boolean startInclusive;
    private boolean endInclusive;

    public TimeRange(Date starting, Date ending) {
        if(starting.after(ending))
            throw new IllegalArgumentException("Starting time should not be behind of ending time!");
        this.starting = starting;
        this.ending = ending;
        this.startInclusive = false;
        this.endInclusive = false;
    }

    public TimeRange(String starting, String ending) {
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = DateUtil.parseLocalDate(starting);
            endDate = DateUtil.parseLocalDate(ending);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(startDate.after(endDate))
            throw new IllegalArgumentException("Starting time should not be behind of ending time!");
        this.starting = startDate;
        this.ending = endDate;
        this.startInclusive = false;
        this.endInclusive = false;
    }

    public TimeRange(Date starting, Date ending, boolean startInclusive, boolean endInclusive) {
        if(starting.after(ending))
            throw new IllegalArgumentException("Starting time should not be behind of ending time!");
        this.starting = starting;
        this.ending = ending;
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    public Date getStarting() {
        return this.starting;
    }

    public Date getEnding() {
        return this.ending;
    }

    /**
     * @return time interval between starting time and ending time, in milliseconds.
     */
    public long getInterval() {
        return this.ending.getTime() - this.starting.getTime();
    }
}
