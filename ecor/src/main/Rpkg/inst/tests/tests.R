# TODO: Add comment
# 
# Author: dmitriy
###############################################################################




test1 <- function () {

  hconf <- ecor.hconf()
  ecor.input(hconf) <- "/home/dmitriy/ecortests"
  ecor.output(hconf) <- "/home/dmitriy/ecortestsout"

  mapfun <- function ( key,value ) { 
	  ecor.collect(key,value)
  }
  
  
  hjob <- ecor.hjob(hconf, mapfun)
  	
}

test2 <- function () {
	d <- proto.desc('com.inadco.ecoadapters.ecor.tests.codegen.Tests$EcorTest')

	rmsg <- list()
	rmsg$idbuff <- charToRaw('012345566')
	rmsg$str <- 'this is a string'
	
	# can use either atomic vector or a list with integral 
	# subscripting
	rmsg$rvector <- c(5,2)
	rmsg$rvector2 <- list()
	rmsg$rvector2[[1]] <- 1
	rmsg$rvector2[[2]] <- 2
	rmsg$timeAttr <- as.numeric(Sys.time())*1000
	rmsg$nested1 <- list()
	rmsg$nested1$name <- "nested1"
    rmsg$nested2 <- list()
	rmsg$nested2[[1]] <- list()
	rmsg$nested2[[1]]$name <- "nested2-1"
    rmsg$nested2[[2]]<-list()
	rmsg$nested2[[2]]$name <- "nested2-2"
	rmsg$floatval <- 32.0
	rmsg$intval <- 32
	rmsg$boolval <- T
	

	p <- proto.toProtoBldr( rmsg, d )
	p <- proto.toProtoRaw( rmsg, d)
	
	rl <- proto.fromProtoRaw(p,d)

	expect_that(rawToChar(rl$idbuff),equals('012345566'))
	expect_that(rl$clickThru$advertiserAccountNumber, equals('this is a strin'))
}

context("prototests")
test_that("protoconversions",test2())


