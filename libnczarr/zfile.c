/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file
 * @internal The netCDF-4 file functions.
 *
 * This file is part of netcdf-4, a netCDF-like interface for NCZ, or
 * a ZARR backend for netCDF, depending on your point of view.
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"
#include "zfilter.h"

/* Sync from NC_VAR_INFO_T.fill_value to attribute _FillValue */
int
NCZ_copy_var_to_att(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NC_ATT_INFO_T* att)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/* Sync from Attribute_FillValue to NC_VAR_INFO_T.fill_value */
int
NCZ_copy_att_to_var(NC_FILE_INFO_T* file, NC_ATT_INFO_T* att, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    return THROW(stat);
}


