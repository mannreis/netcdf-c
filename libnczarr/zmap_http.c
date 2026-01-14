/*
 *	Copyright 2026, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"

#include <curl/curl.h>

#define NCZM_HTTP_V1 1

/* Define the "subclass" of NCZMAP */
typedef struct ZHTTPMAP {
    NCZMAP map;
    CURL * curlhandle;
    char *errmsg;
} ZHTTPMAP;

/* Forward */
static NCZMAP_API nczhttpapi;
static int zhttpcreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp);
static int zhttpopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp);
static int zhttptruncate(const char *url);
static int zhttpexists(NCZMAP* map, const char* key);
static int zhttplen(NCZMAP* map, const char* key, size64_t* lenp);
static int zhttpread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content);
static int zhttpwrite(NCZMAP* map, const char* key, size64_t count, const void* content);
static int zhttpclose(NCZMAP* map, int deleteit);

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
    nclog(NCLOGWARN, "The current Zarr HTTP implementation is READ-ONLY! There is no way to create items.");
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
}

static int zhttpopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_ECANTREAD;
    ZTRACE(6,"path=%s mode=%d flag=%llu",path,mode,flags);

    zhttpinitialize();

    ZHTTPMAP *zhttp = NULL;

    if( NULL == (zhttp=calloc(1,sizeof(ZHTTPMAP))) ) {
        stat = NC_ENOMEM;
        goto done;
    }

    zhttp->map.format = NCZM_HTTP;
    zhttp->map.mode = mode;
    zhttp->map.flags = flags;
    zhttp->map.api = (NCZMAP_API*)&nczhttpapi;

    if (NULL == (zhttp->curlhandle = curl_easy_init())) {
        stat = NC_ECURL;
    }

    NCURI *uri = calloc(1, sizeof(uri));
    if (NC_NOERR != ncuriparse(path, &uri)){
        stat = NC_EURL;
        goto done;
    }

    nullfree(uri->fraglist);
    uri->fraglist = NULL;

    zhttp->map.url = ncuribuild(uri, NULL, uri->path, 0);

    CURLcode result;
    if (CURLE_OK != (result=curl_easy_setopt(zhttp->curlhandle, CURLOPT_URL, zhttp->map.url))){
        nclog(NCLOGERR,"Unable to set url [%s]", curl_easy_strerror(result));
        goto done;
    }

    if ( CURLE_OK != (result = curl_easy_setopt(zhttp->curlhandle, CURLOPT_NOBODY, 1L))) {
        nclog(NCLOGERR, "Unable to set HEAD on request [%s]", curl_easy_strerror(result));
        goto done;
    }

    if ( CURLE_OK != (result = curl_easy_setopt(zhttp->curlhandle, CURLOPT_FOLLOWLOCATION, 1L)) ) {
        nclog(NCLOGERR, "Unable to set follow redirects HEAD on request [%s]", curl_easy_strerror(result));
        goto done;
    }

    if ( CURLE_OK != (result = curl_easy_perform(zhttp->curlhandle))) {
        nclog(NCLOGERR, "Unable to perform request! [%s]", curl_easy_strerror(result));
        goto done;
    }

    char *effective_url = NULL;
    result = curl_easy_getinfo(zhttp->curlhandle, CURLINFO_EFFECTIVE_URL, &effective_url);
    if (!result && effective_url)
    {
        nclog(NCLOGDEBUG, "Effective url: %s", effective_url);
        nullfree(zhttp->map.url);
        zhttp->map.url = strdup(effective_url);
    }

    stat = NC_NOERR;

    if (mapp) {
        *mapp = (NCZMAP *)zhttp;
        zhttp = NULL;
    }

done:
    if(zhttp){
        zhttpclose((NCZMAP *)zhttp, 0);
    }
    ncurifree(uri);
    return ZUNTRACE(stat);
}

static int zhttptruncate(const char *url)
{
    int stat = NC_NOERR;
    ZTRACE(6,"url=%s",url);
    nclog(NCLOGWARN, "The current Zarr HTTP implementation is READ-ONLY! There is no way to truncate data.");
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

    ZHTTPMAP *zhttp = (ZHTTPMAP*) map;
    CURLcode result;
    CURL *c = zhttp->curlhandle;

    curl_easy_reset(c);

    size_t url_len = strlen(zhttp->map.url) + strlen(key) + 1;
    char *url = calloc(1, url_len);
    if (url == NULL)
    {
        stat = NC_ENOMEM;
        goto done;
    }
    memset(url, 0, url_len);
    memcpy(url, zhttp->map.url, strlen(zhttp->map.url));
    memcpy(url+strlen(url), key, strlen(key));

    if (CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_URL, url)))
    {
        nclog(NCLOGERR, "Unable to set url [%s]", curl_easy_strerror(result));
        stat = NC_ECURL;
        goto done;
    }

    result = curl_easy_setopt(c, CURLOPT_NOBODY, 1L);

    result = curl_easy_perform(c);
    if (CURLE_OK != result)
    {
        nclog(NCLOGERR, "Unable perform HEAD request on url \"%s\" [%s]",  url, curl_easy_strerror(result));
        stat = NC_ECURL;
        goto done;
    }

    curl_off_t cl;
    result = curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &cl);
    nclog(NCLOGDEBUG, "CONTENT LENGTH: %"CURL_FORMAT_CURL_OFF_T" (%x)", cl, cl);
    if (cl < 0) {
        nclog(NCLOGERR, "Unable to determine size for [%s]", url);
        stat = NC_ECANTREAD;
        goto done;
    } else if (cl == 0) {
        stat = NC_EEMPTY;
        goto done;
    }

    if (lenp && cl > 0) {
        *lenp = (size64_t)cl;
        printf("ret: %llu\n", *lenp);
    }

    long code = 0;
    if (CURLE_OK != (result = curl_easy_getinfo(c, CURLINFO_HTTP_CODE, &code)))
    {
        nclog(NCLOGERR, "Unable to perform a valid HEAD request [%s] (%s)", url, curl_easy_strerror(result));
        stat = NC_ECANTREAD;
        goto done;
    }

    switch (code){
        case 404:
            stat = NC_ENOOBJECT;
            break;
        default:
            break;
        }
done:
    nullfree(url);
    return ZUNTRACE(stat);
}

// curl callback function to copy data received
static size_t content_copy(char *data, size_t _size_of_one, size_t realsize, void *bufp)
{
    NCbytes *content_buf = (NCbytes*)bufp;
    if (ncbytesavail(content_buf,realsize) == 0) {
        // This should not happen!
       nclog(NCLOGERR, "Unexpected data received, WON'T reallocate to accomodate excendent!");
       return realsize;
    }

    ncbytesappendn(content_buf, data, realsize);
    return realsize;
}

static int zhttpread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZHTTPMAP *zhttp = (ZHTTPMAP *)map;
    ZTRACE(6, "map=%s key=%s start=%llu count=%llu", zhttp->map.url, key, start, count);
    CURLcode result;
    CURL *c = zhttp->curlhandle;
    curl_easy_reset(c);

    size_t url_len = strlen(zhttp->map.url) + strlen(key) + 1;
    char *url = NULL;
    if( NULL == (url = calloc(1, url_len)))
    {
        stat = NC_ENOMEM;
        goto done;
    }

    memset(url, 0, url_len);
    memcpy(url, zhttp->map.url, strlen(zhttp->map.url));
    memcpy(url+strlen(url), key, strlen(key));

    char range[64] = {0}; //at most each size_t takes 21 chars
    if (start + count > start) { // avoid sending request with overflowed ranges
        sprintf(range, "%llu-%llu", start, (start + count));
    }

    NCbytes * content_buf = ncbytesnew();
    ncbytessetcontents(content_buf, content, start+count, start);

    if (CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_URL, url)) ||
        CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_HTTPGET, 1L)) ||
        CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_RANGE, &range)) ||
        CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_WRITEDATA, (void *)content_buf)) ||
        CURLE_OK != (result = curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, content_copy)))
    {
        nclog(NCLOGERR, "Unable to prepare GET request [%s] (%s)", url, curl_easy_strerror(result));
        stat = NC_ECANTREAD;
        goto done;
    }

    if (CURLE_OK != (result = curl_easy_perform(c)))
    {
        nclog(NCLOGERR, "Unable to perform GET request [%s] (%s)", url, curl_easy_strerror(result));
        stat = NC_ECANTREAD;
        goto done;
    }

done:
    nullfree(url);
    return ZUNTRACE(stat);
}

static int zhttpwrite(NCZMAP* map, const char* key, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s count=%llu",map->url,key,count);
    nclog(NCLOGWARN, "The current Zarr HTTP implementation is READ-ONLY!!!");
    stat = NC_ECANTWRITE;
    return ZUNTRACE(stat);
}

static int zhttpclose(NCZMAP* map, int deleteit)
{
    int stat = NC_NOERR;
    ZHTTPMAP *zhttp = (ZHTTPMAP *)map;
    ZTRACE(6,"map=%s deleteit=%d",zhttp->map.url, deleteit);
    if (deleteit) {
        stat = NC_ECANTREMOVE;
    }
    curl_easy_cleanup(zhttp->curlhandle);
    return ZUNTRACE(stat);
}

static int zhttpsearch(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s prefix0=%s",map->url,prefix);
    nclog(NCLOGWARN, "The current Zarr HTTP implementation is READ-ONLY! There is no way to list/search.");
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
