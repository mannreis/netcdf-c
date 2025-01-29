
/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"
#include "nczoh.h"

#undef ZOHDEBUG

#undef DEBUG
#define DEBUGERRORS

#define NCZM_ZOH_V1 1

#define ZOH_PROPERTIES (0)

/* Define the "subclass" of NCZMAP */
typedef struct ZOHMAP
{
	NCZMAP map;
	NCZOH_RESOURCE_INFO resource;
	void *client;
	char *errmsg;
} ZOHMAP;

/* Forward */
static NCZMAP_API nczohapi; // c++ will not allow static forward variables
//static int zohlen(NCZMAP *map, const char *key, size64_t *lenp);
//static void zohinitialize(void);
void* NC_zohcreateclient(NCZOH_RESOURCE_INFO* context);

static void
errclear(ZOHMAP *zoh)
{
	if (zoh)
	{
		if (zoh->errmsg)
			free(zoh->errmsg);
		zoh->errmsg = NULL;
	}
}

#ifdef DEBUGERRORS
static void
reporterr(ZOHMAP *zoh)
{
	if (zoh)
	{
		if (zoh->errmsg)
		{
			nclog(NCLOGERR, zoh->errmsg);
		}
		errclear(zoh);
	}
}
#else
#define reporterr(map)
#endif

void zohmap_free(ZOHMAP * zoh) {
	if (zoh != NULL)
	{
		nullfree(zoh->map.url)
		//NC_zohdestroy(zoh->client, 0);
	}
}
ZOHMAP * zohmap_new(const char * path, mode_t mode, size64_t flags){
	int stat = NC_NOERR;
	
	NCURI * url = NULL;
	/* Parse the URL */
	if ((stat = ncuriparse(path, &url)))
		goto done;
	if (url == NULL)
	{
		stat = NC_EURL;
		goto done;
	}

	/*  Allocate map */
	ZOHMAP * zoh = NULL;
	if ((zoh = (ZOHMAP *)calloc(1, sizeof(ZOHMAP))) == NULL)
	{
		stat = NC_ENOMEM;
		goto done;
	}

	zoh->map.format = NCZM_ZOH;
	zoh->map.url = strdup(path);
	zoh->map.mode = mode;
	zoh->map.flags = flags;
	zoh->map.api = (NCZMAP_API *)&nczohapi;
	
	
	// /* Convert to canonical path-style */
	// if ((stat = NC_zohurlprocess(url, &zoh->resource, NULL)))
	// 	goto done;
	// /* Verify root path */
	// if (zoh->resource.key == NULL)
	// {
	// 	stat = NC_EURL;
	// 	goto done;
	// }

	// zoh->client = NC_zohcreateclient(&zoh->resource);
	return zoh;
done:
	zohmap_free(zoh);
	return NULL;
}
/* Define the Dataset level API */

static int zohinitialized = 0;

static void
zohinitialize(void)
{
	if (!zohinitialized)
	{
		ZTRACE(7, NULL);
		zohinitialized = 1;
		(void)ZUNTRACE(NC_NOERR);
	}
}

void NCZ_zohfinalize(void)
{
	zohinitialized = 0;
}

static int
zohcreate(const char *path, mode_t mode, size64_t flags, void *parameters, NCZMAP **mapp)
{
	return NC_EZARRMETA;
}

static int
zohopen(const char *path, mode_t mode, size64_t flags, void *parameters, NCZMAP **mapp)
{
	return NC_EZARRMETA;
}

static int
zohtruncate(const char *url)
{
	return NC_EZARRMETA;
}

static int
zohclose(NCZMAP *map, int deleteit)
{
	int stat = NC_NOERR;
	ZTRACE(6, "map=%s deleteit=%d", map->url, deleteit);
	return NC_EZARRMETA;
}

static int
zohexists(NCZMAP *map, const char *key)
{
	return NC_EZARRMETA;
}

static int
zohlen(NCZMAP *map, const char *key, size64_t *lenp)
{
	return NC_EZARRMETA;
}


static int
zohread(NCZMAP *map, const char *key, size64_t start, size64_t count, void *content)
{
	return NC_EZARRMETA;
}

static int
zohwrite(NCZMAP *map, const char *key, size64_t count, const void *content)
{
	return NC_EZARRMETA;
}

static int
zohlist(NCZMAP *map, const char *prefix, NClist *matches)
{
	return NC_EZARRMETA;
}
	
static int
zohlistall(NCZMAP *map, const char *prefix, NClist *matches)
{
	return NC_EZARRMETA;
}


/* External API objects */

/* Dispatcher for ZOH */
NCZMAP_DS_API zmap_zoh;
NCZMAP_DS_API zmap_zoh = {
    .version = NCZM_ZOH_V1,
    .features = ZOH_PROPERTIES,
    .create = zohcreate,
    .open = zohopen,
    .truncate = zohtruncate,
};

static NCZMAP_API
    nczohapi = {
	.version = NCZM_ZOH_V1,
	.close = zohclose,
	.exists = zohexists,
	.len = zohlen,
	.read = zohread,
	.write = zohwrite,
	.list = zohlist,
	.listall = zohlistall,
};