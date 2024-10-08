/*! \file Test added as part of JIRA ticket NCF-326.

Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
2015, 2016, 2017, 2018
University Corporation for Atmospheric Research/Unidata.

See \ref copyright file for more info.

  The test was provided by Ellen Johnson at Mathworks.

  See https://bugtracking.unidata.ucar.edu/browse/NCF-326
  for more information.
*/
#include <config.h>
#include <stdio.h>
#include <string.h>
#include <netcdf.h>

#define FILE_NAME "tst_put_vars.nc"

/* 3D matrix, 6 x 4 x 3 */

#define NDIMS 3
#define X_LEN 6
#define Y_LEN 4
#define Z_LEN 3

/* Handle errors by printing an error message */
#define ERR(e) {printf("Error: %s\n", nc_strerror(e)); return 2;}
int
main()
{
    size_t start[NDIMS] = {0, 0, 0};
    size_t count[NDIMS] = {X_LEN, Y_LEN, Z_LEN};
    ptrdiff_t stride[NDIMS] = {1, 1, 1};
    float mydata[X_LEN * Y_LEN * Z_LEN];
    int i;
    int retval;

    int ncid, varid;
    int dimids[NDIMS];

    for (i = 0; i < (X_LEN * Y_LEN * Z_LEN); i++)
        mydata[i] = (float)i;

    /* create the file in NetCDF-4 format */
    if ((retval = nc_create(FILE_NAME, NC_NETCDF4, &ncid)))
        ERR(retval);

    /* define dimensions */
    if ((retval = nc_def_dim(ncid, "time", X_LEN, &dimids[0])))
        ERR(retval);
    if ((retval = nc_def_dim(ncid, "lat", Y_LEN, &dimids[1])))
        ERR(retval);
    if ((retval = nc_def_dim(ncid, "lon", NC_UNLIMITED, &dimids[2])))
        ERR(retval);

   /* define the variable */
    if ((retval = nc_def_var(ncid, "data", NC_FLOAT, NDIMS, dimids, &varid)))
        ERR(retval);

    /* end define mode */
    if ((retval = nc_enddef(ncid)))
        ERR(retval);

    /* This will fail. */
    if ((retval = nc_put_vars_float(ncid, varid, NULL, count, stride,
                                    mydata)) != NC_EINVALCOORDS)
        ERR(99);

    /* write data */
    if ((retval = nc_put_vars_float(ncid, varid, start, count, stride, mydata)))
        ERR(retval);

    /* close the file */
    if ((retval = nc_close(ncid)))
        ERR(retval);

    printf("\n\n*** SUCCESS writing example file %s!\n", FILE_NAME);
    return 0;
}
