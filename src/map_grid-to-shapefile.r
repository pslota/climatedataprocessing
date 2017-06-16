#===========================================================
# Name: Map Grid to Shapefile
# Author: D. Broman, USBR Technical Service Center
# Last Modified: 2017-06-15
# Description: Creates a correspondence file between
# a grid (e.g. climate projections) and shapefile
# (e.g. river basin)
#===========================================================
library(dplyr)
library(readr)
library(stringr)
library(data.table)
library(tools)
library(ncdf4)
library(gdata)
require(mapdata)
require(rgdal)
require(raster)
require(rgeos)
require(sp)

options(digits = 8)
open_shp_to_spdf = function(shp, key = 'id'){

	# shpfile - shapefile (.shp)
	# key = 'index' - optional argument to set index column name
	# 	or specify. This column will be renamed 'index' for 
	# 	consistency
	require(rgdal)
	require(stringr)
	require(tools)
	shp_raw = unlist(str_split(file_path_sans_ext(shp), pattern = '/'))
	shpindmax = length(shp_raw)
	shp_filepath = paste(shp_raw[1:shpindmax-1], collapse = '/')
	shp_file = shp_raw[shpindmax]
	shprd = readOGR(dsn = shp_filepath, layer = shp_file)
	shprd_att = shprd@data
	shprd_att$id = rownames(shprd_att)
	indexind = match(key, names(shprd_att))
	names(shprd_att)[indexind] = 'index'
	areas = data.frame(area=sapply(shprd@polygons, FUN=function(x) {slot(x, 'area')})) %>% setNames('area')
	areas$index = sapply(shprd@polygons, FUN=function(x) {slot(x, 'ID')})
	cpt = t(data.frame(cpt=sapply(shprd@polygons, FUN=function(x) {slot(x, 'labpt')}))) %>% data.frame() %>% setNames(c('lon', 'lat'))
	cpt$index = sapply(shprd@polygons, FUN=function(x) {slot(x, 'ID')})
	shprd_att = shprd_att %>% left_join(areas) %>% left_join(cpt)
	shprd@data = shprd_att
	# fix 'TopologyException: Input geom 0 is invalid: Ring Self-intersection'
	shprd = gBuffer(shprd, byid = T, width = 0)
	return(shprd)
}

#===========================================================
# User Inputs:

#- Working Directory
setwd('...') 
outdir = 'lib/'

#- Grid Shapefile
shp_grid_path = 'C:/Users/dbroman/GIS/LOCA/loca_grd_wwcra'

#- Area Shapefile
shp_area_path = 'C:/Users/dbroman/Projects/Salt River Project SRO/GIS/sac-sma_salt-verde_basinzones'

#- Data Name (for output)
data_name = 'saltverde'

#===========================================================
# Read In Data:
shp_grid = open_shp_to_spdf(shp_grid_path)
shp_area = open_shp_to_spdf(shp_area_path)

#===========================================================
# Process Data:

#- Clip Grid to Bounding Box around Area
b = bbox(shp_area)
b[1, ] = (b[1, ] - mean(b[1, ])) * 1.5 + mean(b[1, ])
b[2, ] = (b[2, ] - mean(b[2, ])) * 1.5 + mean(b[2, ])
lon_min = min(b[1, ])
lon_max = max(b[1, ])
lat_min = min(b[2, ])
lat_max = max(b[2, ])
shp_grid = shp_grid[which(shp_grid@data$lon >= lon_min & shp_grid@data$lon <= lon_max & shp_grid@data$lat >= lat_min & shp_grid@data$lat <= lat_max), ]

#- Intersect Grid and Area Shapefiles
pi = intersect(shp_area, shp_grid)
pi_att = pi@data
pi_att$ID = row.names(pi)
areas = data.frame(area=sapply(pi@polygons, FUN=function(x) {slot(x, 'area')})) %>% setNames('area_pi')
areas$ID = sapply(pi@polygons, FUN=function(x) {slot(x, 'ID')})

#- Calculate Areas
pi_att = pi_att %>% left_join(areas) %>% rename(index = index.1, area_gridbox = area.2, area_area = area.1, area_overlap = area_pi, lon = lon.2, lat = lat.2) %>% dplyr::select(index, lon, lat, area_gridbox, area_area, area_overlap) %>% mutate(area_frac = area_overlap / area_area) %>% ungroup()

#===========================================================
# Save Data:
write.csv(pi_att, paste0(outdir, data_name, '_correspondence_tbl.csv'), row.names = F)
