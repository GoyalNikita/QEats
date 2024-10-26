
/*
 *
 * * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;


import com.crio.qeats.utils.FetchRestaurantsCallable;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;

  ExecutorService executorService = Executors.newFixedThreadPool(10);


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    double srcLat = getRestaurantsRequest.getLatitude();
    double srcLon = getRestaurantsRequest.getLongitude();
    GetRestaurantsResponse responseRestaurants = new GetRestaurantsResponse();
    if (currentTime.isAfter(LocalTime.of(7, 59, 59)) && currentTime.isBefore(LocalTime.of(10, 0, 1))
        || currentTime.isAfter(LocalTime.of(12, 59, 59))
            && currentTime.isBefore(LocalTime.of(14, 0, 1))
        || currentTime.isAfter(LocalTime.of(18, 59, 59))
            && currentTime.isBefore(LocalTime.of(21, 0, 1))) {

      responseRestaurants.setRestaurants(restaurantRepositoryService
          .findAllRestaurantsCloseBy(srcLat, srcLon, currentTime, peakHoursServingRadiusInKms));
    } else {
      responseRestaurants.setRestaurants(restaurantRepositoryService
          .findAllRestaurantsCloseBy(srcLat, srcLon, currentTime, normalHoursServingRadiusInKms));
    }
    return responseRestaurants;

  }


  private boolean isPeakHour(LocalTime currentTime){

    int hour = currentTime.getHour();
    return hour>=8&&hour<=10 || hour>=13&&hour<=14 || hour>=19&&hour<=21;

  }

  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    Double servingRadiusInKms =
        isPeakHour(currentTime) ? peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;

    String searchFor = getRestaurantsRequest.getSearchFor();
    List<List<Restaurant>> listOfRestaurantLists = new ArrayList<>();

    if (!searchFor.isEmpty()) {
      listOfRestaurantLists.add(
          restaurantRepositoryService.findRestaurantsByName(getRestaurantsRequest.getLatitude(),
              getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));

      listOfRestaurantLists.add(restaurantRepositoryService.findRestaurantsByAttributes(
          getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(), searchFor,
          currentTime, servingRadiusInKms));

      listOfRestaurantLists.add(
          restaurantRepositoryService.findRestaurantsByItemName(getRestaurantsRequest.getLatitude(),
              getRestaurantsRequest.getLongitude(), searchFor, currentTime, servingRadiusInKms));

      listOfRestaurantLists.add(restaurantRepositoryService.findRestaurantsByItemAttributes(
          getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(), searchFor,
          currentTime, servingRadiusInKms));

      Set<String> restaurantSet = new HashSet<>();
      List<Restaurant> restaurantList = new ArrayList<>();
      for (List<Restaurant> restoList : listOfRestaurantLists) {
        for (Restaurant restaurant : restoList) {
          if (!restaurantSet.contains(restaurant.getRestaurantId())) {
            restaurantList.add(restaurant);
            restaurantSet.add(restaurant.getRestaurantId());
          }
        }
      }

      return new GetRestaurantsResponse(restaurantList);
    } else {
      return new GetRestaurantsResponse(new ArrayList<>());
    }

  }


  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    Double servingRadiusInKms =
        isPeakHour(currentTime) ? peakHoursServingRadiusInKms : normalHoursServingRadiusInKms;

    String searchFor = getRestaurantsRequest.getSearchFor();
    List<List<Restaurant>> listOfRestaurantLists = new ArrayList<>();
    List<FetchRestaurantsCallable> callables = new ArrayList();

    if (!searchFor.isEmpty()) {
      callables.add(new FetchRestaurantsCallable("byRestaurantAttr", getRestaurantsRequest,
          restaurantRepositoryService, searchFor, currentTime, servingRadiusInKms));

      callables.add(new FetchRestaurantsCallable("byRestaurantName", getRestaurantsRequest,
          restaurantRepositoryService, searchFor, currentTime, servingRadiusInKms));

      callables.add(new FetchRestaurantsCallable("byItemName", getRestaurantsRequest,
          restaurantRepositoryService, searchFor, currentTime, servingRadiusInKms));

      callables.add(new FetchRestaurantsCallable("byItemAttr", getRestaurantsRequest,
          restaurantRepositoryService, searchFor, currentTime, servingRadiusInKms));
      Set<String> restaurantSet = new HashSet<>();
      List<Restaurant> restaurantList = new ArrayList<>();
      try {
        List<Future<List<Restaurant>>> futureResults = executorService.invokeAll(callables);
        shutdownAndAwaitTermination(executorService);


        for (Future<List<Restaurant>> futureResult : futureResults) {
          for (Restaurant restaurant : futureResult.get()) {
            if (!restaurantSet.contains(restaurant.getRestaurantId())) {
              restaurantList.add(restaurant);
              restaurantSet.add(restaurant.getRestaurantId());
            }
          }
        }
      } catch (InterruptedException ie) {

      } catch (ExecutionException ee) {

      }



      return new GetRestaurantsResponse(restaurantList);
    } else {
      return new GetRestaurantsResponse(new ArrayList<>());
    }
  }

  private void shutdownAndAwaitTermination(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }


}

