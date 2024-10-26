
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


public class Restaurant {
  @JsonIgnore
  private @Getter @Setter String id;
  private @Getter @Setter String restaurantId;
  private @Getter @Setter String name;
  private @Getter @Setter String city;
  private @Getter @Setter String imageUrl;
  private @Getter @Setter Double latitude;
  private @Getter @Setter Double longitude;
  private @Getter @Setter String opensAt;
  private @Getter @Setter String closesAt;
  private @Getter @Setter List<String> attributes;
}

