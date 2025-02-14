/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCZOH_H
#define NCZOH_H 1

/* Opaque Handles */
struct NClist;

typedef struct NCZOH_RESOURCE_INFO {
    char *protocol; /* http or https */
    char *host; /* non-null if other*/
    char* port;
    char* key; /* resource*/
} NCZOH_RESOURCE_INFO;

/* Opaque Types */
struct NClist;
struct NCglobalstate;

#ifdef __cplusplus
extern "C" {
#endif

/* API for ncs3sdk_XXX.[c|cpp] */
EXTERNL int NC_zohinitialize(void);
EXTERNL int NC_zohfinalize(void);
EXTERNL void* NC_zohcreateclient(const NCZOH_RESOURCE_INFO* context);
EXTERNL int NC_zohinfo(void* client, const char* pathkey, unsigned long long* lenp, char** errmsgp);
EXTERNL int NC_zohread(void* client, const char* pathkey, unsigned long long start, unsigned long long count, void* content, char** errmsgp);
EXTERNL int NC_zohwriteobject(void* client0, const char* bucket, const char* pathkey, unsigned long long count, const void* content, char** errmsgp);
EXTERNL int NC_zohdestroy(void* client, char** errmsgp);
EXTERNL int NC_zohtruncate(void* client, const char* bucket, const char* prefix, char** errmsgp);
EXTERNL int NC_zohlist(void* client, const char* bucket, const char* prefix, size_t* nkeysp, char*** keysp, char** errmsgp);
EXTERNL int NC_zohlistall(void* client, const char* bucket, const char* prefixkey0, size_t* nkeysp, char*** keysp, char** errmsgp);
EXTERNL int NC_zohdelete(void* client, const char* bucket, const char* pathkey, char** errmsgp);

#ifdef __cplusplus
}
#endif

#endif /*NCZOH_H*/
