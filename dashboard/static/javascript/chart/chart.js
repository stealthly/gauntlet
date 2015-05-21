(function () {
  'use strict';

  angular.module('dashboardApp').directive('appChart', function(){
    return {
      restrict: 'E',
      templateUrl: '/static/javascript/chart/chart.html',
    };
  });
}());
