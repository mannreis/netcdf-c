
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

void* NC_zohcreateclient(const NCZOH_RESOURCE_INFO * context);

typedef enum HTTPVerb {
	HTTPNONE=0,
	HTTPGET=1,
	HTTPPUT=2,
	HTTPPOST=3,
	HTTPHEAD=4,	
	HTTPDELETE=5 // Not used
} HTTPVerb;


struct MemoryChunk {
	char *memory;
    size_t size; // Common for both GET and PUT
    union {
        size_t allocated_size; // For GET (total allocated memory)
        size_t read_pos; // For PUT (current read position)
    } u;
};

struct request {
	CURL * curlhandle;
	char   httpverb[16];
	struct curl_slist *curlheaders;
	struct MemoryChunk mem;
};

typedef struct result {
	long http_code;
} result;

typedef struct zoh_client_internal
{
	struct request req[NUM_REQUESTS];
	struct CURL_M *curlmhandle;
	int still_running;
	int num_concurrent_requests;
} zoh_client;

CURLcode ncrc_curl_setopts(CURL *curlh);

//HTTP related forward declarations
static int request_setup(struct request *req, const char *url, HTTPVerb verb);

static int zohclose(NCZMAP *map, int deleteit);

static int validate_handle(struct request *req, const char *url);

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

	zoh->resource.protocol = url->protocol?strdup(url->protocol):"http";
	zoh->resource.host = strdup(url->host);
	zoh->resource.port = url->port?strdup(url->port):NULL;
	zoh->resource.key = strdup(url->path); 

    zoh->map.format = NCZM_ZOH;
    zoh->map.url = strdup(path);
    zoh->map.mode = mode;
	zoh->map.api = &nczohapi;
    zoh->map.flags = flags;
	
	zoh->client = NC_zohcreateclient(&zoh->resource);// No request is performed

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
	ZOHMAP *zoh = (ZOHMAP*)map;
	zoh_client *client = zoh->client;

	zohdestroyclient(client);

	ZTRACE(6, "map=%s deleteit=%d", map->url, deleteit);
	return NC_NOERR;
}

int request_perform(struct request * req, struct result *res) {
	CURLcode code = curl_easy_perform(req->curlhandle);
	if (CURLE_OK != code) {
		// extract HTTP code if available
		curl_easy_getinfo(req->curlhandle, CURLINFO_RESPONSE_CODE, res->http_code);
	}
	return code;
}

/* Prefix key with path to root to make true key */
static int
maketruekey(const char* rootpath, const char* key, char** truekeyp) {
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
	
	ZTRACE(6,"map=%s key=%s"map->url,key);

	NCURI url = {
		.protocol = zoh->resource.protocol,
		.host = zoh->resource.host,
		.port = zoh->resource.port,
		.path = NULL,
	};

	if ((stat = maketruekey(zoh->resource.key, key, &url.path))){
		goto done;
	}

	if ((stat = validate_handle(&client->req[0], url.path))){
		goto done;
	}

	char * url_s = ncuribuild(&url,NULL,NULL,NCURIPATH);
	request_setup(&client->req[0], url_s, HTTPHEAD);
	struct result res = {0};
	stat = request_perform(&client->req[0], &res);
	if(stat){
		nclog(NCLOGERR, "Failed on checking %s [%ld]", key, res.http_code );
	}

done:
	nullfree(fullkey);
	nullfree(url_s);
	return stat;
}

static int request_reset(struct request * req) {
	if (req == NULL)
		return NC_NOERR;

	if (req->curlheaders)
	{
		curl_slist_free_all(req->curlheaders);
		req->curlheaders = NULL;
	}
	
	nullfree(req->mem.memory);
	req->mem.memory = NULL;
	req->mem.size = 0;
	req->mem.u.allocated_size = 0;

	return NC_NOERR;
}

static int request_cleanup(struct request * req) {
	if (req == NULL)
		return NC_NOERR;
	curl_easy_cleanup(req->curlhandle);
	req->curlhandle = NULL;

	return request_reset(req);
}

static int
zohlen(NCZMAP *map, const char *key, size64_t *lenp)
{
	int stat = NC_NOERR;
	ZOHMAP *zoh = (ZOHMAP *)map;
	zoh_client *client = zoh->client;
	struct request *req = &(client->req[0]);
	char *fullkey = NULL;
	
	ZTRACE(6,"map=%s key=%s"map->url,key);

	NCURI url = {
		.protocol = zoh->resource.protocol,
		.host = zoh->resource.host,
		.port = zoh->resource.port,
		.path = NULL,
	};

	if ((stat = maketruekey(zoh->resource.key, key, &url.path))){
		goto done;
	}

	if ((stat = validate_handle(req, url.path))){
		goto done;
	}

	char * url_s = ncuribuild(&url,NULL,NULL,NCURIPATH);

	request_setup(req, url_s, HTTPHEAD);
	struct result res = {0};
	stat = request_perform(req, &res);
	if(stat){
		nclog(NCLOGERR, "Failed getting length of %s [%ld]", key, res.http_code);
		goto done;
	}

	*lenp = req->mem.u.allocated_size;

done:
	nullfree(fullkey);
	request_reset(req);
	nullfree(url_s);
	return stat;
}


static int
zohread(NCZMAP *map, const char *key, size64_t start, size64_t count, void *content)
{
	int stat = NC_NOERR;
	ZOHMAP *zoh = (ZOHMAP*)map;
	zoh_client *client = zoh->client;
	struct request *req = &(client->req[0]);
	char *fullkey = NULL;
	
	ZTRACE(6,"map=%s key=%s"map->url,key);

	NCURI url = {
		.protocol = zoh->resource.protocol,
		.host = zoh->resource.host,
		.port = zoh->resource.port,
		.path = NULL,
	};

	if ((stat = maketruekey(zoh->resource.key, key, &url.path))){
		goto done;
	}

	if ((stat = validate_handle(req, url.path))){
		goto done;
	}

	char * url_s = ncuribuild(&url,NULL,NULL,NCURIPATH);
	request_setup(req, url_s, HTTPGET);
	struct result res = {0};
	stat = request_perform(req, &res);
	if(stat || req->mem.memory == NULL){
		nclog(NCLOGERR, "Failed reading %s [%ld]", key, res.http_code);
		goto done;
	}

	memcpy(content, req->mem.memory, req->mem.size);

done:
	request_reset(req);
	nullfree(fullkey);
	nullfree(url_s);
	return stat;
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


void zohdestroyclient(void * client) {
	zoh_client * zohclient = (zoh_client *)client;
	struct request *req = NULL;

	if (zohclient != NULL) {
		for (int i = 0 ;  i < NUM_REQUESTS; i++) {
			request_cleanup(&zohclient->req[i]);
		}
		curl_multi_cleanup(zohclient->curlmhandle);
		free(zohclient);
	}
	return;
}

void *
NC_zohcreateclient(const NCZOH_RESOURCE_INFO* context)
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
		request_reset(&client->req[i]);
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



static int validate_handle(struct request * req, const char* url) {
    int stat = NC_NOERR;
	if (req == NULL || req->curlhandle == NULL){
		stat = NC_EINVAL;
		goto done;
	}

done:
	return (stat);
}



// Callback to extract Content-Length
size_t curl_callback_allocate_content_length (char *buffer, size_t size, size_t nitems, void *userdata) {
    size_t totalSize = size * nitems;
	struct MemoryChunk * mem = (struct MemoryChunk *)userdata;

	if (strncasecmp(buffer, "Content-Length:", 15) == 0)
	{
		if (mem == NULL) {
			nclog(NCLOGERR, "MemoryChunk is NULL");
		}
        mem->u.allocated_size = (size_t)atol(buffer + 15);
		nullfree(mem->memory);
		mem->memory = calloc(1,mem->u.allocated_size + 1);  // Allocate once
		if (!mem->memory)
		{
			free(mem);
			fprintf(stderr, "Failed to allocate memory\n");
			return 0; // Abort
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

    // Reallocate memory if needed - Callback header already pre-allocated memory
	if (mem->u.allocated_size < mem->size + totalSize) { 
    	char *new_memory = realloc(mem->memory, mem->size + totalSize + 1);
    	if (new_memory == NULL) {
     	   fprintf(stderr, "Failed to allocate memory for GET response.\n");
      	  return 0;
    	}
    	mem->memory = new_memory;
		printf("REALLOCATION ON WRITE: %zu\n", totalSize);
		mem->u.allocated_size = mem->size + totalSize + 1;
	}

	memcpy(&(mem->memory[mem->size]), contents, totalSize);
    mem->size += totalSize;
    mem->memory[mem->size] = 0;  // Null-terminate the string

    return totalSize;
}

static int request_setup(struct request * req, const char* url, HTTPVerb verb) {
	int stat = NC_NOERR;
	CURL* curlh = req->curlhandle;

	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_URL, url)){
		stat = NC_EINVAL;
		goto done;
	}
	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION, 1L)){
		stat = NC_EINVAL;
		goto done;
	}
	if (CURLE_OK != ncrc_curl_setopts(curlh)){
		stat = NC_EINVAL;
		goto done;
	}

	switch (verb) {
		case HTTPGET:
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPGET, 1L)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, curl_callback_allocate_content_length)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, (void *)&req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, (void *) req->curlheaders)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, curl_callback_writetomemory)){
				stat = NC_EINVAL;
				goto done;
			}
        	if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_WRITEDATA, (void *)&req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPPUT:
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_UPLOAD, 1L)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_READDATA, (void*)&req->mem)){
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
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, (void*)&req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, curl_callback_allocate_content_length)){
				stat = NC_EINVAL;
				goto done;
			}
			break;
		case HTTPDELETE:
			if( CURLE_OK != curl_easy_setopt(curlh, CURLOPT_CUSTOMREQUEST, "DELETE")){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERDATA, (void*)&req->mem)){
				stat = NC_EINVAL;
				goto done;
			}
			if (CURLE_OK != curl_easy_setopt(curlh, CURLOPT_HEADERFUNCTION, curl_callback_allocate_content_length)){
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

	const char * verb_s = verbtext(verb);
	memset(req->httpverb, 0, sizeof(req->httpverb));
	assert(strlen(verb_s) < sizeof(req->httpverb));
	memcpy(req->httpverb, verb_s, strlen(verb_s));

done:
	return (stat);
}

CURLcode ncrc_curl_setopts(CURL *curlh) {
	CURLcode stat = CURLE_OK;
	char *value = NULL;
	value = NC_rclookup("HTTP.SSL.CAINFO", NULL, NULL);
	if (value != NULL){
		if((CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_CAINFO, value)))){
			goto done;
		}
	}
	value = NC_rclookup("HTTP.VERBOSE", NULL, NULL);
	if (value != NULL && CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_VERBOSE, value[0]!='0'?1L:0L))){
		nclog(NCLOGERR,"Unable to set HTTP.VERBOSE: %s\n",value);
		goto done;
	}
	value = NC_rclookup("HTTP.KEEPALIVE",NULL,NULL);
	if(value != NULL && strlen(value) != 0) {
		if((CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_TCP_KEEPALIVE, 1L)))){
			nclog(NCLOGERR,"Unable to set HTTP.KEEPALIVE: %s\n",value);
			goto done;
		}
		
		/* The keepalive value is of the form 0 or n/m,
		where n is the idle time and m is the interval time;
		setting either to zero will prevent that field being set.*/
		if(strcasecmp(value,"on")!=0) {
			unsigned long idle=0;
			unsigned long interval=0;
			if(sscanf(value,"%lu/%lu",&idle,&interval) != 2) {
				nclog(NCLOGERR,"Illegal KEEPALIVE VALUE: %s\n",value);
			}
			if((CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_TCP_KEEPIDLE, (long)idle)))){
				goto done;
			}
			if ((CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_TCP_KEEPINTVL, (long)interval)))){
				goto done;
			}
		}
	}

	char *token = NULL;
	if (NULL != (token = getenv("NETCDF_ZOH_TOKEN"))){
		if(CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_HTTPAUTH, (long)CURLAUTH_BEARER))){
			nclog(NCLOGERR, "Failed to set Authentication for cURL\n");
			goto done;
		}
		if(CURLE_OK != (stat = curl_easy_setopt(curlh, CURLOPT_XOAUTH2_BEARER, token))){
			nclog(NCLOGERR, "Failed to set token in cURL\n");
			goto done;
		}
	}

done:
	return stat;
}