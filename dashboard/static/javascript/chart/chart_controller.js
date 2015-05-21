(function () {
  'use strict';

  angular.module('dashboardApp').controller('ChartController', ['$scope', 'events', function($scope, events) {
    $scope.startFetching = function() {
      var conn = new WebSocket("ws://localhost:8080/events");

      conn.onclose = function() {
        console.log("Connection closed.");
      };

      conn.onopen = function() {
        console.log("Connection opened.");
      };

      conn.onmessage = function(e) {
        $scope.$apply(function() {
          var message = JSON.parse(e.data);
          $scope.addEvent(message);
          if ($scope.charts[message.consumerId].svg) {
            $scope.charts[message.consumerId].svg.html('');
          }
          $scope.$applyAsync(function() {
            $scope.drawChart(message.consumerId);
          });
        });
      };
    };

    $scope.addChart = function(consumerId) {
      $scope.charts[consumerId] = {events: {}};
    };

    $scope.addEvent = function(event) {
      if (!$scope.charts[event.consumerId]) {
        $scope.addChart(event.consumerId);
      }
      var events = $scope.charts[event.consumerId].events;
      if (!events[event.eventName]) {
        events[event.eventName] = {};
      }
      if (!events[event.eventName][event.operation]) {
        events[event.eventName][event.operation] = [];
      }
      events[event.eventName][event.operation].push(event)
      $scope.charts[event.consumerId].events = events;
    };

    events.fetch(function(data){
      $scope.charts = {};

      if (data) {
        data.forEach(function(event){
          $scope.addEvent(event);
        });
      }

      for(var consumerId in $scope.charts) {
        $scope.drawChart(consumerId);
      }

      $scope.startFetching();
    });

    $scope.drawChart = function(consumerId) {
      var chart = $scope.charts[consumerId];

      var margin = {top: 20, right: 20, bottom: 30, left: 50},
          width = 380 - margin.left - margin.right,
          height = 200 - margin.top - margin.bottom;

      chart.x = d3.scale.linear()
          .range([0, width]);

      chart.y = d3.scale.linear()
          .range([height, 0]);

      var xAxis = d3.svg.axis()
          .scale(chart.x)
          .orient("bottom");

      var yAxis = d3.svg.axis()
          .scale(chart.y)
          .orient("left");

      chart.line = d3.svg.line()
          .x(function(d) { return chart.x(d.second); })
          .y(function(d) { return chart.y(d.value); });

      chart.svg = d3.select("#chart_" + consumerId)
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      var maxSecondDomain = 0;
      var maxValueDomain = 0;


      var allEvents = [];
      for (eventName in chart.events) {
        for (operation in chart.events[eventName]){
          allEvents = allEvents.concat(chart.events[eventName][operation])
        }
      }

      chart.x.domain(d3.extent(allEvents, function(d) {
        return d.second;
      }));
      chart.y.domain(d3.extent(allEvents, function(d) {
        return d.value;
      }));

      chart.svg.append("g")
          .attr("class", "x axis")
          .attr("transform", "translate(0," + height + ")")
          .call(xAxis);

      chart.svg.append("g")
          .attr("class", "y axis")
          .call(yAxis)
        .append("text")
          .attr("transform", "rotate(-90)")
          .attr("y", 6)
          .attr("dy", ".71em")
          .style("text-anchor", "end")
          .text("Value");


      var operations = ["avg10sec", "avg30sec", "avg1min", "avg5min", "avg10min", "avg15min"];

      for (var eventName in chart.events) {
        for (var operation in chart.events[eventName]) {
          var color = d3.scale.category20();
          color.domain(operations);
          chart.svg.append("path")
              .datum(chart.events[eventName][operation])
              .attr("class", "line")
              .attr("d", chart.line)
              .style("stroke", function(d) { return color(d[0].operation) });
        };
      };

      $scope.charts[consumerId] = chart;
    };
  }]);
}());
