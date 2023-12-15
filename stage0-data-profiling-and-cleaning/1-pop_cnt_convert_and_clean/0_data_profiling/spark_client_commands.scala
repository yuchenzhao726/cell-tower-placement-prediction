// --package leverage maven to manage dependency  
spark-shell --deploy-mode client --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0"

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.vector.Extent
import geotrellis.vector.Point

// file path on local filesystem can be used if on spark client
val inputTiff = "bdad_proj/pop_cnt.tif"

// file path on hdfs   
val outputDir = "bdad_proj/pop_cnt"  

// data profiling

// this is local io, it cannot be used for cluster mode
val geoTiff: SinglebandGeoTiff = GeoTiffReader.readSingleband(inputTiff)
val tile = geoTiff.tile
tile.cols
tile.rows
//tile.histogramDouble
val hist = tile.histogram
val stats = hist.statistics
val validCellPercentage = stats.get.dataCells / (tile.cols * tile.rows)




