(function () {
  'use strict';

  angular.module('dashboardApp').controller('ChartController', ['$scope', '$timeout', 'events', function($scope, $timeout, events) {
    $scope.fields = ["value", "count"];

    $scope.startFetching = function() {
      var conn = new WebSocket("ws://" + window.location.host + "/events");

      conn.onclose = function() {
        console.log("Connection closed.");
      };

      conn.onopen = function() {
        console.log("Connection opened.");
        $scope.startRendering();
      };

      conn.onmessage = function(e) {
        $scope.$apply(function() {
          var message = JSON.parse(e.data);
          $scope.addEvent(message);
        });
      };
    };

    $scope.startRendering = function() {
      setInterval(function(){
        for (var chartId in $scope.charts) {
          if (!$scope.charts[chartId].rendered) {
            if ($scope.charts[chartId].svg) {
              $scope.charts[chartId].svg.selectAll('*').remove();
            }

            $scope.drawChart(chartId);
          }
        }
      }, 1000);
    };

    $scope.addChart = function(consumerId, field) {
      $scope.charts[consumerId + field] = {events: {}, allEvents: [], field: field, consumerId: consumerId};
    };

    $scope.addEvent = function(event) {
      for (var i=0; i<$scope.fields.length; i++){
        var chartId = event.consumerId + $scope.fields[i];
        if (!$scope.charts[chartId]) {
          $scope.addChart(event.consumerId, $scope.fields[i]);
        }
        var events = $scope.charts[chartId].events;
        if (!events[event.partition]) {
          events[event.partition] = [];
        }
        events[event.partition].push(event);
        $scope.charts[chartId].allEvents.push(event);
        $scope.charts[chartId].events = events;
        $scope.charts[chartId].rendered = false;
      }
    };

    events.fetch(function(data){
      $scope.charts = {};

      if (data) {
        data.forEach(function(event){
          $scope.addEvent(event);
        });
      }

      for(var chartId in $scope.charts) {
        $scope.drawChart(chartId);
      }

      $scope.startFetching();
    });

    $scope.render = function(last) {
      if (last) {
        $timeout(function() {
          for(var chartId in $scope.charts) {
            $scope.charts[chartId].rendered = false;
            $scope.charts[chartId].init = false;
            $scope.drawChart(chartId);
          }
        }, 0);
      }
    };

    $scope.drawChart = function(chartId) {
      var chart = $scope.charts[chartId];
      if (!chart.init) {
        chart.margin = {top: 20, right: 20, bottom: 30, left: 50};
        chart.width = 1100 - chart.margin.left - chart.margin.right;
        chart.height = 500 - chart.margin.top - chart.margin.bottom;

        chart.x = d3.scale.linear()
            .range([0, chart.width]);

        chart.y = d3.scale.linear()
            .range([chart.height, 0]);

        chart.xAxis = d3.svg.axis()
            .scale(chart.x)
            .orient("bottom");

        chart.yAxis = d3.svg.axis()
            .scale(chart.y)
            .orient("left");

        chart.line = d3.svg.line()
            .x(function(d) { return chart.x(d.second); })
            .y(function(d) { return chart.y(d[chart.field]); });

        chart.svg = d3.select("#chart_" + chart.field + "_" + chart.consumerId)
            .attr("width", chart.width + chart.margin.left + chart.margin.right)
            .attr("height", chart.height + chart.margin.top + chart.margin.bottom)
          .append("g")
            .attr("transform", "translate(" + chart.margin.left + "," + chart.margin.top + ")");
        chart.init = true;
      }

      chart.x.domain(d3.extent(chart.allEvents, function(d) {
        return d.second;
      }));
      chart.y.domain(d3.extent(chart.allEvents, function(d) {
        return d[chart.field];
      }));

      chart.svg.append("g")
          .attr("class", "x axis")
          .attr("transform", "translate(0," + chart.height + ")")
          .call(chart.xAxis);

      chart.svg.append("g")
          .attr("class", "y axis")
          .call(chart.yAxis)
          .append("text")
          .attr("transform", "rotate(-90)")
          .attr("y", 6)
          .attr("dy", ".71em")
          .style("text-anchor", "end")
          .text(chart.field);

      var color = d3.scale.category20();
      var partitions = new Array(120);
      for (var i = 0; i<partitions.length; i++) {
        partitions[i] = (i+1).toString();
      }

      var tooltip = d3.select(".tooltip");

      var styling = function(d) {
        tooltip.transition()
          .duration(300)
          .style("opacity", 0);
        tooltip.transition()
          .duration(50)
          .style("opacity", 0.9);
        path.style("stroke-width", "5px");
        tooltip.html("<p>Partition: " + d[0].partition + "</p><p>Topic: " + d[0].topic + "</p>")
          .style("left", d3.event.pageX + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      };
      var unstyling = function(d) {
        tooltip.transition()
          .duration(2000)
          .style("opacity", 0);
        path.style("stroke-width", "2.5px");
      };

      for (var partition in chart.events) {
        color.domain(partitions);
        var path = chart.svg.append("path")
            .datum(chart.events[partition])
            .attr("class", "line")
            .attr("d", chart.line)
            .style("stroke", function(d) { return color(partition) });
        path.on("mouseover", styling);
        path.on("mouseout", unstyling);
      }

      $scope.charts[chartId] = chart;
      $scope.charts[chartId].rendered = true;
    };
  }]);
}());
