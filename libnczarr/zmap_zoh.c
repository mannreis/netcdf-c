
/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"
#include "nczoh.h"

#include <curl/curl.h>

#ifndef CURLPIPE_MULTIPLEX
#define CURLPIPE_MULTIPLEX 0
#endif
 
#undef ZOHDEBUG

#undef DEBUG
#define DEBUGERRORS

#define NCZM_ZOH_V1 1

#define ZOH_PROPERTIES (0)

#define NUM_REQUESTS 1

/* Define the "subclass" of NCZMAP */
typedef struct ZOHMAP
{
	NCZMAP map;
	NCZOH_RESOURCE_INFO resource;
	void *client;
	char *errmsg;
} ZOHMAP;

/* Forward */
static NCZMAP_API nczohapi;

void* NC_zohcreateclient(const NCZOH_RESOURCE_INFO context);

typedef enum HTTPVerb {
	HTTPNONE=0,
	HTTPGET=1,
	HTTPPUT=2,
	HTTPPOST=3,
	HTTPHEAD=4,	
	HTTPDELETE=5 // Not used
} HTTPVerb;


struct request {
	CURL * curlhandle;
	char   httpverb[16];
	struct curl_slist *curlheaders;
	struct MemoryChunk *mem;
};

typedef struct zoh_client_internal {
	struct request req[NUM_REQUESTS];
	struct CURL_M *curlmhandle;
	int still_running;
	int num_concurrent_requests;
} zoh_client;

struct MemoryChunk {
	char *memory;
    size_t size; // Common for both GET and PUT
    union {
        size_t allocated_size; // For GET (total allocated memory)
        size_t read_pos; // For PUT (current read position)
    } u;
};

//HTTP related forward declarations
static int request_setup(struct request *req, const char *url, HTTPVerb verb);

static int zohclose(NCZMAP *map, int deleteit);

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

static int zohinitialized = 0;

static void
zohinitialize(void)
{
	if (!zohinitialized)
	{
		ZTRACE(7, NULL);
		zohinitialized = 1;
	    curl_global_init(CURL_GLOBAL_DEFAULT);
		(void)ZUNTRACE(NC_NOERR);
	}
}

void zohfinalize(void)
{
	zohinitialized = 0;
	curl_global_cleanup();
}

static int
zohcreate(const char *path, mode_t mode, size64_t flags, void *parameters, NCZMAP **mapp)
{
	return NC_EZARRMETA;
}

static int
zohopen(const char *path, mode_t mode, size64_t flags, void *parameters, NCZMAP **mapp)
{
	int stat = NC_NOERR;
	ZOHMAP *zoh = NULL;
	NCURI *url = NULL;

	ZTRACE(6,"path=%s mode=%d flags=%llu",path,mode,flags);
    
	if(!zohinitialized) {
		zohinitialize();
	}

    if((stat = ncuriparse(path,&url))) {
		goto done;
	}

	if (url == NULL || url->host == NULL) {
		stat = NC_EURL;
		goto done;
	}
	
    if((zoh = (ZOHMAP*)calloc(1,sizeof(ZOHMAP))) == NULL) {
		stat = NC_ENOMEM;
		goto done;
	}

	zoh->resource.host = strdup(url->host);
	zoh->resource.port = url->port?strdup(url->port):NULL;
	zoh->resource.key = strdup(url->path); 

    zoh->map.format = NCZM_ZOH;
    zoh->map.url = strdup(path);
    zoh->map.mode = mode;
	zoh->map.api = &nczohapi;
    zoh->map.flags = flags;
	
	zoh->client = NC_zohcreateclient(zoh->resource);// No request is performed

	if(mapp) *mapp = (NCZMAP*)zoh;
done:
    reporterr(zoh);
    ncurifree(url);
    if(stat) zohclose((NCZMAP*)zoh,0);
    return ZUNTRACE(stat);
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

int request_perform(struct request * req) {
	return curl_easy_perform(req->curlhandle);
}

/* Prefix key with path to root to make true key */
static int
maketruekey(const char* rootpath, const char* key, char** truekeyp)
	{
	int  stat = NC_NOERR;
	NCbytes* truekey = NULL;
	size_t rootlen,keylen;

	if(truekeyp == NULL) goto done;

	truekey = ncbytesnew();
	if(truekey == NULL) {stat = NC_ENOMEM; goto done;}
	if(rootpath[0] != '/')    
		ncbytescat(truekey,"/"); /* force leading '/' */
	rootlen = strlen(rootpath);

	/* Force no trailing '/' */
	if(rootpath[rootlen-1] == '/') rootlen--;
	ncbytesappendn(truekey,rootpath,rootlen);
	ncbytesnull(truekey);

	keylen = nulllen(key);    
	if(keylen > 0) {
		if(key[0] != '/') /* force '/' separator */
		ncbytescat(truekey,"/");
		ncbytescat(truekey,key);
	ncbytesnull(truekey);
	}
	/* Ensure no trailing '/' */
	if(ncbytesget(truekey,ncbyteslength(truekey)-1) == '/')
		ncbytessetlength(truekey,ncbyteslength(truekey)-1);
	ncbytesnull(truekey);    
	if(truekeyp) *truekeyp = ncbytesextract(truekey);

	done:
	ncbytesfree(truekey);
	return stat;
	}

static int
zohexists(NCZMAP *map, const char *key)
{
	int stat = NC_NOERR;
	ZOHMAP *zoh = (ZOHMAP*)map;
	zoh_client *client = zoh->client;
	char *fullkey = NULL;
	NCURI url = {0};
		
	ZTRACE(6,"map=%s key=%s",map->url,key);
	
	if ((stat = maketruekey(zoh->resource.key, key, &fullkey))){
		goto done;
	}

	CURL *curlh = client->req[0].curlhandle;

	url.host = zoh->resource.host;
	if (zoh->resource.port != NULL) {
		url.port = zoh->resource.port;
	}
	url.path = fullkey;

	char * url_s = ncuribuild(&url,NULL,NULL,0);
	request_setup(&client->req[0], url_s, HTTPHEAD);
	stat = request_perform(&client->req[0]);
done:
	nullfree(fullkey);
	nullfree(url_s);
	return NC_EZARRMETA;
}

static int
zohlen(NCZMAP *map, const char *key, size64_t *lenp)
{
done:
	return NC_EZARRMETA;
}


static int
zohread(NCZMAP *map, const char *key, size64_t start, size64_t count, void *content)
{
done:
	return NC_EZARRMETA;
}

static int
zohwrite(NCZMAP *map, const char *key, size64_t count, const void *content)
{
done:
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



void *
NC_zohcreateclient(const NCZOH_RESOURCE_INFO context)
{
	int stat = NC_NOERR;
	zoh_client *client = (zoh_client *)calloc(1, sizeof(zoh_client));
	
	client->curlmhandle = curl_multi_init();
	client->num_concurrent_requests = 0;
	client->still_running = 0;
	for (int i = 0; i < NUM_REQUESTS; i++) {
		
		CURL * curlh = curl_easy_init();

		if (curlh == NULL) {
			stat = NC_EINVAL;
			goto done;
		}

		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0)){
			stat = NC_EINVAL;
			goto done;
		}

		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_FAILONERROR, 1L)){
			stat = NC_EINVAL;
			goto done;
		}

		client->req[i].curlhandle = curlh;
		client->req[i].curlheaders = NULL;

		// Track easy handles;
		curl_multi_add_handle(&client->curlmhandle, client->req[i].curlhandle);
	}
	return (void*) client;
done:
	return NULL;
}

void * _zohresethandles(zoh_client * client) {
	int stat = NC_NOERR;
	client->num_concurrent_requests = 0;
	client->still_running = 0;
	for (int i = 0; i < NUM_REQUESTS; i++) {
		CURL * curlh = client->req[i].curlhandle;
		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_NOBODY, NULL)) {
			stat = NC_EINVAL;
			goto done;
		}

		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, NULL)){
			stat = NC_EINVAL;
			goto done;
		}
	
		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, NULL)){
			stat = NC_EINVAL;
			goto done;
		}

		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_URL, NULL)){
			stat = NC_EINVAL;
			goto done;
		}

		if(CURLE_OK != curl_easy_setopt(curlh, CURLOPT_CUSTOMREQUEST, NULL)){
			stat = NC_EINVAL;
			goto done;
		}

		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPGET, 1L)){
			stat = NC_EINVAL;
			goto done;
		}

		/* clear any Range */
		if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_RANGE, NULL)){
			stat = NC_EINVAL;
			goto done;
		}
	}
done:
    return (stat);
}


static const char*
verbtext(HTTPVerb verb)
{
    switch(verb) {
    case HTTPGET: return "GET";
    case HTTPPUT: return "PUT";
    case HTTPPOST: return "POST";
    case HTTPHEAD: return "HEAD";
    case HTTPDELETE: return "DELETE";
    default: break;
    }
    return NULL;
}



static int
validate_handle(struct request * req, const char* url) {
    int stat = NC_NOERR;
	if (req == NULL || req->curlhandle == NULL){
		stat = NC_EINVAL;
		goto done;
	}

done:
	return (stat);
}

// Callback to extract Content-Length
size_t HeaderGetCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
    size_t totalSize = size * nitems;
    struct MemoryChunk *mem = (struct MemoryChunk *)userdata;

    if (strncasecmp(buffer, "Content-Length:", 15) == 0) {
        mem->u.allocated_size = (size_t)atol(buffer + 15);
        printf("Content-Length: %zu bytes\n", mem->u.allocated_size);
        mem->memory = malloc(mem->u.allocated_size + 1);  // Allocate once
        if (!mem->memory) {
            fprintf(stderr, "Failed to allocate memory\n");
            return 0;  // Abort
        }
    }
    return totalSize;
}

// Callback for reading data during PUT request
size_t curl_callback_readfrommemory(void *ptr, size_t size, size_t nmemb, void *userp) {
    struct MemoryChunk *mem = (struct MemoryChunk *)userp;
    size_t max_bytes = size * nmemb;

    if (mem->u.read_pos >= mem->size) {
        return 0; // No more data to send
    }

    size_t remaining_bytes = mem->size - mem->u.read_pos;
    size_t bytes_to_copy = (remaining_bytes < max_bytes) ? remaining_bytes : max_bytes;

    memcpy(ptr, mem->memory + mem->u.read_pos, bytes_to_copy);
    mem->u.read_pos += bytes_to_copy;

    return bytes_to_copy;
}

// Callback for writing data during GET request
size_t curl_callback_writetomemory(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t totalSize = size * nmemb;
    struct MemoryChunk *mem = (struct MemoryChunk *)userp;

    // Reallocate memory if needed
    char *new_memory = realloc(mem->memory, mem->size + totalSize + 1);
    if (new_memory == NULL) {
        fprintf(stderr, "Failed to allocate memory for GET response.\n");
        return 0;
    }
    mem->memory = new_memory;

    memcpy(&(mem->memory[mem->size]), contents, totalSize);
    mem->size += totalSize;
    mem->memory[mem->size] = 0;  // Null-terminate the string
    mem->u.allocated_size = mem->size;  // Track the allocated size

    return totalSize;
}

static int request_setup(struct request * req, const char* url, HTTPVerb verb) {
	int stat = NC_NOERR;
	CURL* curlh = req->curlhandle;

	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_URL, url)){
		stat = NC_EINVAL;
		goto done;
	}

	switch (verb) {
		case HTTPGET:
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPGET, 1L)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, HeaderGetCallback)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, (void *)req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, curl_callback_writetomemory)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEDATA, (void *)req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPPUT:
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_UPLOAD, 1L)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_READDATA, (void*)req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_READFUNCTION, curl_callback_readfrommemory)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPHEAD:
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_NOBODY, 1L)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, HeaderGetCallback)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPDELETE:
			if( CURLE_OK != curl_easy_setopt(curlh, CURLOPT_CUSTOMREQUEST, "DELETE")){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, HeaderGetCallback)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPPOST:
		default:
			stat = NC_EINVAL;
			goto done;
			break;
	}

done:
	return (stat);
}

http_head(struct request *req, const char * url) {

}
