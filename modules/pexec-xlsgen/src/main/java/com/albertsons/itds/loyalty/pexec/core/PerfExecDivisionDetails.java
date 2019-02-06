package com.albertsons.itds.loyalty.pexec.core;

import java.util.LinkedHashMap;

public class PerfExecDivisionDetails {

  private LinkedHashMap<String, Division> divisions = new LinkedHashMap<>();

  public Division getDivision(String id) {
    return divisions.get(id);
  }

  public void addDivition(String id, Division division) {
    divisions.put(id, division);
  }

  public static class Division {

    public Division(String name, String description) {
      super();
      this.name = name;
      this.description = description;
    }

    private String name;

    private String description;

    /** @return the name */
    public String getName() {
      return name;
    }

    /** @return the description */
    public String getDescription() {
      return description;
    }
  }

  /** @return the divisions */
  public LinkedHashMap<String, Division> getDivisions() {
    return divisions;
  }

  /** @param divisions the divisions to set */
  public void setDivisions(LinkedHashMap<String, Division> divisions) {
    this.divisions = divisions;
  }
}
