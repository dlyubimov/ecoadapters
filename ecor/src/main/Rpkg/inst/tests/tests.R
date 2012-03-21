# TODO: Add comment
# 
# Author: dmitriy
###############################################################################




test1 <- function () {

  	
}

testSW <- function() { 
	library(ecor)
	sfw <- ecor.SequenceFileW$new("/temp/swftest1.seq")
	tryCatch({
				#so vectorizaton and recycling
				# is now supported by append. 
				# we can write batches from vectors at once. 
					system.time(sfw$append(rep("KEYKEY", length=3000),"BBVAL"))
			}, 
			finally = {
				sfw$close()
			}
			)
	
}

test2 <- function () {
  library(ecor)
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
  p1 <- proto.toProtoRaw(rl,d)

  #	expect_that(rawToChar(rl$idbuff),equals('012345566'))
#	expect_that(rl$clickThru$advertiserAccountNumber, equals('this is a string'))
  
  valW <- ecor.ProtoWritable$new(
      'com.inadco.ecoadapters.ecor.tests.codegen.Tests$EcorTest')
  
  infile <- "/temp/swftest1.seq"
	sfw <- ecor.SequenceFileW$new(infile,
			valWritable=valW)
	
	tryCatch({
				#so vectorizaton and recycling
				# is now supported by append. 
				# we can write batches from vectors at once.
				for ( i in 1:1000)	sfw$append("",rmsg)
			}, 
			finally = {
				sfw$close()
			}
	)
  
  hconf <- ecor.hconf()
  ecor.input(hconf) <- infile
  ecor.output(hconf) <- "/temp/rmr-out"
  
  mapfun <- function ( key,value ) { 
    ecor.collect(key,value)
  }
  
  
  hjob <- ecor.hjob(hconf, mapfun)
  
	
}

context("prototests")
test_that("protoconversions",test2())


