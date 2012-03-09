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

test2 <- function () {
	d <- proto.desc('hdfs://localhost:11010/data/inadco/protolib/inadco-fas.protodesc?msg=inadco.logs.pipeline.FASClick')

	rmsg <- list()
	rmsg$clickId <- charToRaw('012345566')
	rmsg$sessionId <- charToRaw('kdfklsdf')
	rmsg$clickThru <- list()
	rmsg$clickThru$advertiserAccountNumber <- 'kwerklsdfsdf'
	
	rmsg$serverTimeUTC <- as.numeric(Sys.time())*1000
	
	p <- proto.toProtoBldr( rmsg, d )
	p <- proto.toProtoRaw( rmsg, d)
	
	rl <- proto.fromProtoRaw(p,d)

	expect_that(rawToChar(rl$sessionId),equals('kdfklsdf'))
	expect_that(rl$clickThru$advertiserAccountNumber, equals('kwerklsdfsdf'))
}

context("prototests")
test_that("protoconversions",test2())


