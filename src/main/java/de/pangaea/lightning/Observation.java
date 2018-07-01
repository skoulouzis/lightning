package de.pangaea.lightning;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "madeBySensor",
    "observedProperty",
    "qualityOfObservation",
    "id",
    "hasResult",
    "resultTime",
    "featureOfInterest",
    "resultUnit",
    "resultValue"
})
public class Observation {

    @JsonProperty("madeBySensor")
    private String madeBySensor;
    @JsonProperty("observedProperty")
    private String observedProperty;
    @JsonProperty("qualityOfObservation")
    private Integer qualityOfObservation;
    @JsonProperty("id")
    private Object id;
    @JsonProperty("hasResult")
    private HasResult hasResult;
    @JsonProperty("resultTime")
    private String resultTime;
    @JsonProperty("featureOfInterest")
    private String featureOfInterest;
    @JsonProperty("resultUnit")
    private String resultUnit;
    @JsonProperty("resultValue")
    private Float resultValue;

    @JsonProperty("madeBySensor")
    public String getMadeBySensor() {
        return madeBySensor;
    }

    @JsonProperty("madeBySensor")
    public void setMadeBySensor(String madeBySensor) {
        this.madeBySensor = madeBySensor;
    }

    @JsonProperty("observedProperty")
    public String getObservedProperty() {
        return observedProperty;
    }

    @JsonProperty("observedProperty")
    public void setObservedProperty(String observedProperty) {
        this.observedProperty = observedProperty;
    }

    @JsonProperty("qualityOfObservation")
    public Integer getQualityOfObservation() {
        return qualityOfObservation;
    }

    @JsonProperty("qualityOfObservation")
    public void setQualityOfObservation(Integer qualityOfObservation) {
        this.qualityOfObservation = qualityOfObservation;
    }

    @JsonProperty("id")
    public Object getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(Object id) {
        this.id = id;
    }

    @JsonProperty("hasResult")
    public HasResult getHasResult() {
        return hasResult;
    }

    @JsonProperty("hasResult")
    public void setHasResult(HasResult hasResult) {
        this.hasResult = hasResult;
    }

    @JsonProperty("resultTime")
    public String getResultTime() {
        return resultTime;
    }

    @JsonProperty("resultTime")
    public void setResultTime(String resultTime) {
        this.resultTime = resultTime;
    }

    @JsonProperty("featureOfInterest")
    public String getFeatureOfInterest() {
        return featureOfInterest;
    }

    @JsonProperty("featureOfInterest")
    public void setFeatureOfInterest(String featureOfInterest) {
        this.featureOfInterest = featureOfInterest;
    }

    @JsonProperty("resultUnit")
    public String getResultUnit() {
        return resultUnit;
    }

    @JsonProperty("resultUnit")
    public void setResultUnit(String resultUnit) {
        this.resultUnit = resultUnit;
    }

    @JsonProperty("resultValue")
    public Float getResultValue() {
        return resultValue;
    }

    @JsonProperty("resultValue")
    public void setResultValue(Float resultValue) {
        this.resultValue = resultValue;
    }

}
