package com.crio.qeats.utils;

import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.Callable;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;

public class FetchRestaurantsCallable implements Callable<List<Restaurant>> {

    private String fetchString;
    GetRestaurantsRequest getRestaurantsRequest;
    RestaurantRepositoryService restaurantRepositoryService;
    String searchFor;
    Double servingRadiusInKms;
    LocalTime currentTime;
    public FetchRestaurantsCallable(String fetchString,GetRestaurantsRequest getRestaurantsRequest,
    RestaurantRepositoryService restaurantRepositoryService,String searchFor
    ,LocalTime currentTime,Double servingRadiusInKms){
        this.fetchString = fetchString;
        this.getRestaurantsRequest = getRestaurantsRequest;
        this.restaurantRepositoryService  = restaurantRepositoryService;
        this.searchFor = searchFor;
        this.currentTime =currentTime;
        this.servingRadiusInKms
         = servingRadiusInKms
         ;
    }
    @Override
    public List<Restaurant> call() throws Exception {
       if(this.fetchString.equals("byRestaurantName")){
        return restaurantRepositoryService.findRestaurantsByName(
              getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(),
              searchFor, currentTime, servingRadiusInKms);
       }else if(this.fetchString.equals("byRestaurantAttr")){
        return restaurantRepositoryService
          .findRestaurantsByAttributes(getRestaurantsRequest.getLatitude(),
              getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms);
       }else if(this.fetchString.equals("byItemName")){
        return restaurantRepositoryService
          .findRestaurantsByItemName(getRestaurantsRequest.getLatitude(),
              getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms);
       }else if(this.fetchString.equals("byItemAttr")){
        return restaurantRepositoryService
          .findRestaurantsByItemAttributes(getRestaurantsRequest.getLatitude(),
              getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms);
       }
       return null;
    }  
}
