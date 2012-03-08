# TODO: Add comment
# 
# Author: dmitriy
###############################################################################




test1 <- function () {
	.ecor.init()
  hconf <- ecor.hconf()
  ecor.input(hconf) <- "/home/dmitriy/ecortests"
  ecor.output(hconf) <- "/home/dmitriy/ecortestsout"

  mapfun <- function ( key,value ) { 
	  ecor.collect(key,value)
  }
  
  
  hjob <- ecor.hjob(hconf, mapfun)
  	
}




