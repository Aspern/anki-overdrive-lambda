'use strict';

var app = angular.module('app', ['PostService']);
var response;

app.controller('MyCtrl', function($scope , $timeout, SendPostReq){

  

  $scope.sendReq = function (url,data) {
    
    response = SendPostReq.sendPost(url,data);

    response.success(function (response) {
        console.log('Url sent');
      });

    response.error(function (response) {
        console.log('Error');
      });

    if(data == 'AC-S')
    {
    		console.log("starting Anti collision");
    }

    else if(data == 'AC-I')
    {
    		console.log("Stoping Anti collision");

    }

    else if(data = "C-S")
    {
    		console.log("starting collision");
    	
    }

    else if(data = "C-I")
    {
    		console.log("Stoping collision");
    	
    }

	
	};


	
	
	
});