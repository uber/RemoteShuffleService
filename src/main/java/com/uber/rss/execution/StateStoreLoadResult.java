package com.uber.rss.execution;

public class StateStoreLoadResult {
  private final boolean partialLoad;
  private final long dataItems;
  private final int applications;
  private final int deletedApplications;
  private final int stages;
  private final int corruptedStages;
  private final int deletedStages;

  public StateStoreLoadResult(boolean partialLoad, long dataItems, int applications, int deletedApplications, int stages, int corruptedStages, int deletedStages) {
    this.partialLoad = partialLoad;
    this.dataItems = dataItems;
    this.applications = applications;
    this.deletedApplications = deletedApplications;
    this.stages = stages;
    this.corruptedStages = corruptedStages;
    this.deletedStages = deletedStages;
  }

  public boolean isPartialLoad() {
    return partialLoad;
  }

  public long getDataItems() {
    return dataItems;
  }

  public int getApplications() {
    return applications;
  }

  public int getDeletedApplications() {
    return deletedApplications;
  }

  public int getStages() {
    return stages;
  }

  public int getCorruptedStages() {
    return corruptedStages;
  }

  public int getDeletedStages() {
    return deletedStages;
  }

  @Override
  public String toString() {
    return "StateStoreLoadResult{" +
        "partialLoad=" + partialLoad +
        ", dataItems=" + dataItems +
        ", applications=" + applications +
        ", deletedApplications=" + deletedApplications +
        ", stages=" + stages +
        ", corruptedStages=" + corruptedStages +
        ", deletedStages=" + deletedStages +
        '}';
  }
}
