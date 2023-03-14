package com.geneea.celery.backends.redis.jedismessage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

// prepare for result message that celery-redis return to client
@JsonIgnoreProperties(ignoreUnknown = true)
public class CeleryResult {
    private String status;
    private Object result;
    private String traceback;
    private List children;
    private String date_done;
    private String task_id;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getTraceback() {
        return traceback;
    }

    public void setTraceback(String traceback) {
        this.traceback = traceback;
    }

    public List getChildren() {
        return children;
    }

    public void setChildren(List children) {
        this.children = children;
    }

    public String getDate_done() {
        return date_done;
    }

    public void setDate_done(String date_done) {
        this.date_done = date_done;
    }

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }
}
