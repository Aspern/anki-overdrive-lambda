var StudentService = angular.module('PostService', [])
StudentService.factory('SendPostReq', ['$http', function ($http) {

    var urlBase = 'http://localhost:8080/rest/setup/ao-adrian/scenario/';


    var StudentDataOp = {};

    StudentDataOp.sendPost = function (url,data) {
        return $http.post(urlBase + url , data);
    };
    return StudentDataOp;

}]);