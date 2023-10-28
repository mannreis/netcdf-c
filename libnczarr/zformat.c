/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zformat.h"

/**************************************************/


extern int NCZF1_initialize(void);
extern int NCZF1_finalize(void);
extern int NCZF2_initialize(void);
extern int NCZF2_finalize(void);
extern int NCZF3_initialize(void);
extern int NCZF3_finalize(void);


/**************************************************/

int
NCZF_initialize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF1_initialize())) goto done;
    if((stat=NCZF2_initialize())) goto done;
    if((stat=NCZF3_initialize())) goto done;
done:
    return THROW(stat);
}

int
NCZF_finalize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF1_finalize())) goto done;
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
NCZF_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->readmeta(file);
    return THROW(stat);
}

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

/* Support lazy read */
int
NCZF_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->readattrs(file,container);
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
