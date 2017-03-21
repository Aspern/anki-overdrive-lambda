'use strict';

var app = angular.module('app', ['PostService']);

app.controller('MyCtrl', function($scope , $timeout, SendPostReq){

var response;
var myEl = angular.element( document.querySelector( '#status' ) );
$scope.date = new Date();

  $scope.sendReq = function (url,data) {
    
    response = SendPostReq.sendPost(url,data);

    response.error(function (response) {
        console.log('Error');
        $timeout(function(){
            
             myEl.append('['+$scope.date+'] System is not connected!<br>'); 

        }, 2000);
      });


    $scope.newDate();

    if(data == 'AC-S')
    {
    		console.log("starting Anti collision");
    		myEl.append('['+$scope.date+']<a class="saving" > Preparing to Start Anti-Collision <span>.</span><span>.</span><span>.</span></a><br>');

    		response.success(function (response) {
        	console.log('Url sent');
        	$timeout(function(){
            
             myEl.append('['+$scope.date+'] Started - Anti-Collision!<br>'); 

        	}, 2000);
      		});
    }

    else if(data == 'AC-I')
    {
    		console.log("Stoping Anti collision");
    		myEl.append('['+$scope.date+']<a class="saving"> Stopping Anti-Collision <span>.</span><span>.</span><span>.</span></a><br>');

    }

    else if(data == "C-S")
    {
    		console.log("starting collision");
    		myEl.append('['+$scope.date+']<a class="saving" > Preparing to Start Collision <span>.</span><span>.</span><span>.</span></a><br>');

    	
    }

    else if(data == "C-I")
    {
    		console.log("Stoping collision");
    		myEl.append('['+$scope.date+']<a class="saving"> Stopping Collision <span>.</span><span>.</span><span>.</span></a><br>');

    	
    }

	
	};


	$scope.newDate = function () {
      // body...

      $scope.date = new Date();

    }


	
	
	
});