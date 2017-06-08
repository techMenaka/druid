package com.tgt.estore.druid.model;

import java.io.Serializable;

/**
 * Created by Menaka on 6/7/17.
 */
public class RequestVO implements Serializable {

    private String dataSource;
    private String fromDate;
    private String toDate;
    private String granularity;
    private String aggrField;

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getFromDate() {
        return fromDate;
    }

    public void setFromDate(String fromDate) {
        this.fromDate = fromDate;
    }

    public String getToDate() {
        return toDate;
    }

    public void setToDate(String toDate) {
        this.toDate = toDate;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
    }

    public String getAggrField() {
        return aggrField;
    }

    public void setAggrField(String aggrField) {
        this.aggrField = aggrField;
    }
}
