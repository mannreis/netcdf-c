/*
 *	Copyright 2026, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"

#define NCZM_HTTP_V1 1

#define ZS3_PROPERTIES (0)

/* Define the "subclass" of NCZMAP */
typedef struct ZHTTPMAP {
    NCZMAP map;
    void* httpclient;
    char* errmsg;
} ZHTTPMAP;

/* Forward */
static NCZMAP_API nczhttpapi;

static void zhttpinitialize(void);

static int zhttpinitialized = 0;

static void
zhttpinitialize(void) {
    if(!zhttpinitialized) {
        zhttpinitialized = 1;
    }
}

/* Dataset API */

static int zhttpcreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZTRACE(6,"path=%s mode=%d flag=%llu",path,mode,flags);
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
}

static int zhttpopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZTRACE(6,"path=%s mode=%d flag=%llu",path,mode,flags);
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
    return ZUNTRACE(stat);
}

static int zhttptruncate(const char *url)
{
    int stat = NC_NOERR;
    ZTRACE(6,"url=%s",s3url);
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
}

/* Object API */

static int zhttpexists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s",map->url,key);
    stat = NC_ECANTREAD;
    return ZUNTRACE(stat);
}

static int zhttplen(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s",map->url,key);
    stat = NC_ECANTREAD;
    return ZUNTRACE(stat);
}

static int zhttpread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s start=%llu count=%llu",map->url,key,start,count);
    stat = NC_ECANTREAD;
    return ZUNTRACE(stat);
}

static int zhttpwrite(NCZMAP* map, const char* key, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s count=%llu",map->url,key,count);
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
}

static int zhttpclose(NCZMAP* map, int deleteit)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s deleteit=%d",map->url, deleteit);
    if (deleteit) {
        stat = NC_ECANTREMOVE;
    }
    return ZUNTRACE(stat);
}

static int zhttpsearch(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s prefix0=%s",map->url,prefix);
    stat = NC_ECANTLIST;
    return ZUNTRACEX(stat, "|matches|=%d", (int)nclistlength(matches));
}


/* External API objects */

NCZMAP_DS_API zmap_http;
NCZMAP_DS_API zmap_http = {
    NCZM_HTTP_V1,
    NCZM_UNLISTABLE,
    zhttpcreate,
    zhttpopen,
    zhttptruncate,
};

static NCZMAP_API
nczhttpapi = {
    NCZM_HTTP_V1,
    zhttpclose,
    zhttpexists,
    zhttplen,
    zhttpread,
    zhttpwrite,
    zhttpsearch,
};
