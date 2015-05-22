(function () {
  angular.module('dashboardApp').factory('events', ['$http', function($http) {
    return {
      fetch: function(callback) {
        $http.get('/event_history').success(callback);
      },
    };
  }]);
}());
