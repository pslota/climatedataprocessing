#===========================================================
# Name: Aggregate Projections
# Author: D. Broman, USBR Technical Service Center
# Last Modified: 2017-06-15
# Description: Aggregates a set of climate projections
# e.g. BCSD or LOCA using a) bounding box, b) lon/lat pairs
# or c) correspondence file from map_grid-to-shapefile.r
#===========================================================
library(dplyr)
library(readr)
library(stringr)
library(data.table)
library(tools)
library(ncdf4)
library(gdata)
library(doParallel)
library(foreach)
options(digits = 8)
#===========================================================
# User Inputs:

#- Working Directory
setwd('/work/dbroman/projects/salt_river_sro/process_loca/') 
indir = 'data/raw'
outdir = 'data/processed'
#- Projection File Lists
file_tbl_path = 'lib/file_tbl.csv'

#- Aggregation [bounding box - bb; lon/lat pairs - lonlat; correspondence table - ct] 
aggregate_flag = 'bb'
aggregate_file_path = 'lib/bounding_box.txt'

#- Dates [timestep_inp: daily - day; monthly - month]
startdate_inp = '1950-01-01'
enddate_inp = '2099-12-31'
timestep_inp = 'day'

#- Data Name (for output)
data_name = 'saltverde'

#- Unit Conversion 
# [mm to in - 0.0393701; in to mm - 25.4]
pr_mult = 0.0393701 

#- Coordinate Adjustment
lon_add = -360

#- Variable Definitions
pr_def = 'pr'
tmin_def = 'tasmin'
tmax_def = 'tasmax'

#===========================================================
# Read Inputs and Setup:
file_tbl = fread(file_tbl_path)
nfile = nrow(file_tbl)

if(aggregate_flag == 'bb'){
	aggregate_tbl = scan(aggregate_file_path)
	lon_min = aggregate_tbl[1]
	lon_max = aggregate_tbl[2]
	lat_min = aggregate_tbl[3]
	lat_max = aggregate_tbl[4]
	narea = 1
} else if(aggregate_flag == 'lonlat'){
	aggregate_tbl = fread(aggregate_file_path) %>% mutate(flag = 1)
	narea = 1
} else if(aggregate_flag == 'ct'){
	aggregate_tbl = fread(aggregate_file_path)
	narea = length(unique(aggregate_tbl$index))
}

startdate = as.Date(startdate_inp)
enddate = as.Date(enddate_inp)
date_vec = seq(from = startdate, to = enddate, by = timestep_inp)
ndate = length(date_vec)

#===========================================================
# Process Data:
for(ifile in 1:nfile){ 
	pr_file_temp = file_tbl$pr[ifile]
	tasmin_file_temp = file_tbl$tasmin[ifile]
	tasmax_file_temp = file_tbl$tasmax[ifile]
	model_temp = file_tbl$model[ifile]
	run_temp = file_tbl$run[ifile]
	scenario_temp = file_tbl$scenario[ifile]

	pr_nc_temp = nc_open(paste0(indir, pr_file_temp))
	tasmin_nc_temp = nc_open(paste0(indir, tasmin_file_temp))
	tasmax_nc_temp = nc_open(paste0(indir, tasmax_file_temp))
	
	lon_vec = ncvar_get(pr_nc_temp, 'lon') + lon_add
	lat_vec = ncvar_get(pr_nc_temp, 'lat')
	pr_temp = ncvar_get(pr_nc_temp, pr_def)
	tasmin_temp = ncvar_get(tasmin_nc_temp, tmin_def)
	tasmax_temp = ncvar_get(tasmax_nc_temp, tmax_def)
	
	for(iarea in 1:narea){
		area_temp = iarea
		filename_temp = paste(model_temp, run_temp, scenario_temp, area_temp, sep = '_')
		if(aggregate_flag == 'bb'){
			lon_inds = which(lon_vec >= lon_min & lon_vec <= lon_max)
			lat_inds = which(lat_vec >= lat_min & lat_vec <= lat_max)
			pr_temp_area = pr_temp[lon_inds, lat_inds, ]
			tasmin_temp_area = tasmin_temp[lon_inds, lat_inds, ]
			tasmax_temp_area = tasmax_temp[lon_inds, lat_inds, ]
			data_temp = data.table(pr = as.numeric(pr_temp_area) * pr_mult, tasmin = as.numeric(tasmin_temp_area), tasmax = as.numeric(tasmax_temp_area), lon = lon_temp, lat = rep(lat_temp, each = nlon), date = rep(date_vec, each = nlon*nlat)) %>% group_by(date) %>% summarise(pr = round(sum(pr), 3), tasmax = round(sum(tasmax), 3), tasmin = round(sum(tasmin), 3))
		} else if(aggregate_flag == 'lonlat'){
			data_temp = data.table(pr = as.numeric(pr_temp_area) * pr_mult, tasmin = as.numeric(tasmin_temp_area), tasmax = as.numeric(tasmax_temp_area), lon = lon_temp, lat = rep(lat_temp, each = nlon), date = rep(date_vec, each = nlon*nlat)) %>% left_join(aggregate_tbl) %>% filter(flag == 1) %>% group_by(date) %>% summarise(pr = round(sum(pr), 3), tasmax = round(sum(tasmax), 3), tasmin = round(sum(tasmin), 3))
		} else if(aggregate_flag == 'cf'){
			aggregate_tbl_temp = aggregate_tbl %>% filter(index == iarea) 
			lon_temp = sort(unique(aggregate_tbl_temp$lon))
			nlon = length(lon_temp)
			lat_temp = sort(unique(aggregate_tbl_temp$lat))
			nlat = length(lat_temp)
			lon_inds = match(lon_temp, lon_vec)
			lat_inds = match(lat_temp, lat_vec)
			pr_temp_area = pr_temp[lon_inds, lat_inds, ]
			tasmin_temp_area = tasmin_temp[lon_inds, lat_inds, ]
			tasmax_temp_area = tasmax_temp[lon_inds, lat_inds, ]
			data_temp = data.table(pr = as.numeric(pr_temp_area) * pr_mult, tasmin = as.numeric(tasmin_temp_area), tasmax = as.numeric(tasmax_temp_area), lon = lon_temp, lat = rep(lat_temp, each = nlon), date = rep(date_vec, each = nlon*nlat)) %>% left_join(aggregate_tbl_temp) %>% filter(!is.na(index)) %>% mutate(pr = pr * area_frac, tasmax = tasmax * area_frac, tasmin = tasmin * area_frac) %>% group_by(date) %>% summarise(pr = round(sum(pr), 3), tasmax = round(sum(tasmax), 3), tasmin = round(sum(tasmin), 3))
		}
		write.csv(data_temp, paste0(outdir, filename_temp, '.csv'), row.names = F)
	}
	nc_close(pr_nc_temp)
	nc_close(tasmax_nc_temp)
	nc_close(tasmin_nc_temp)
}
