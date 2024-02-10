/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zformat.h"
#ifdef ENABLE_NCZARR_FILTERS
#include "zfilter.h"
#endif

/**************************************************/

extern int NCZF2_initialize(void);
extern int NCZF2_finalize(void);
extern int NCZF3_initialize(void);
extern int NCZF3_finalize(void);


/**************************************************/

int
NCZF_initialize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF2_initialize())) goto done;
    if((stat=NCZF3_initialize())) goto done;
done:
    return THROW(stat);
}

int
NCZF_finalize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF2_finalize())) goto done;
    if((stat=NCZF3_finalize())) goto done;
done:
    return THROW(stat);
}

int
NCZF_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->create(file,uri,map);
    return THROW(stat);
}

int
NCZF_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->open(file,uri,map);
    return THROW(stat);
}

int
NCZF_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->close(file);
    return THROW(stat);
}

int
NCZF_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->hdf2codec(file,var,filter);
    return THROW(stat);
}

int
NCZF_codec2hdf(const NC_FILE_INFO_T* file, const NCjson* jfilter, NCZ_Filter* filter, struct NCZ_Plugin* plugin)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->codec2hdf(file,jfilter,filter,plugin);
    return THROW(stat);
}

/**************************************************/

/*
@internal Convert a Zarr data_type spec to a corresponding nc_type.
@param file     - [in] file object
@param dtype    - [in] dtype to convert
@param dalias   - [in] alias of dtype
@param nctypep  - [out] hold corresponding type
@param typelenp - [out] hold corresponding type size (for fixed length strings)
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/
int
NCZF_dtype2nctype(NC_FILE_INFO_T* file, const char* dtype , const char* dalias, nc_type* nctypep, size_t* typelenp, int* endianp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->dtype2nctype(dtype,dalias,nctypep,typelenp,endianp);
    return THROW(stat);
}

/**
@internal Given an nc_type+purezarr+MAXSTRLEN, produce the corresponding Zarr dtype string.
@param file       - [in] file object
@param nctype     - [in] nc_type
@param purezarr   - [in] 1=>pure zarr, 0 => nczarr
@param strlen     - [in] max string length
@param namep      - [out] pointer to hold pointer to the dtype; user frees
@param tagp       - [out] pointer to hold pointer to the nczarr tag
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
NCZF_nctype2dtype(NC_FILE_INFO_T* file, nc_type nctype, int endianness, int purezarr, size_t strlen, char** dnamep, const char** tagp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->nctype2dtype(nctype,endianness,purezarr,strlen,dnamep,tagp);
    return THROW(stat);
}

/**************************************************/
/*
From Zarr V2 Specification:
"The compressed sequence of bytes for each chunk is stored under
a key formed from the index of the chunk within the grid of
chunks representing the array.  To form a string key for a
chunk, the indices are converted to strings and concatenated
with the dimension_separator character ('.' or '/') separating
each index. For example, given an array with shape (10000,
10000) and chunk shape (1000, 1000) there will be 100 chunks
laid out in a 10 by 10 grid. The chunk with indices (0, 0)
provides data for rows 0-1000 and columns 0-1000 and is stored
under the key "0.0"; the chunk with indices (2, 4) provides data
for rows 2000-3000 and columns 4000-5000 and is stored under the
key "2.4"; etc."
*/

/**
 * @param R Rank
 * @param chunkindices The chunk indices
 * @param dimsep the dimension separator
 * @param keyp Return the chunk key string
 */
int
NCZF_buildchunkkey(const NC_FILE_INFO_T* file, int rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->buildchunkkey(rank,chunkindices,dimsep,keyp);
    return THROW(stat);
}

/**************************************************/
/* Compile incoming metadata */

int
NCZF_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->readmeta(file);
    return THROW(stat);
}

/* De-compile incoming metadata */

int
NCZF_writemeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->writemeta(file);
    return THROW(stat);
}
