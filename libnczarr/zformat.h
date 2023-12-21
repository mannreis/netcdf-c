/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 *
 * @author Dennis Heimbigner
 */

#ifndef ZFORMAT_H
#define ZFORMAT_H

/* This is the version of the formatter table. It should be changed
 * when new functions are added to the formatter table. */
#ifndef NCZ_FORMATTER_VERSION
#define NCZ_FORMATTER_VERSION 1
#endif /*NCZ_FORMATTER_VERSION*/

/* struct Fill Values */
#define NCZ_CODEC_ENV_EMPTY_V2 {NCZ_CODEC_ENV_VER, 2}
#define NCZ_CODEC_ENV_EMPTY_V3 {NCZ_CODEC_ENV_VER, 3}

/* Opaque */
struct NCZ_Plugin;

/* This is the dispatch table, with a pointer to each netCDF
 * function. */
typedef struct NCZ_Formatter {
    int nczarr_format;
    int zarr_format;
    int dispatch_version; /* Version of the dispatch table */
    int (*create)    (NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
    int (*open)      (NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
    int (*close)     (NC_FILE_INFO_T* file);
    int (*readmeta)  (NC_FILE_INFO_T* file);
    int (*writemeta) (NC_FILE_INFO_T* file);
    int (*readattrs) (NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, struct NCZ_AttrInfo**);
    int (*buildchunkkey)(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);
    int (*codec2hdf) (const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, struct NCZ_Plugin* plugin);
    int (*hdf2codec) (const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter);
} NCZ_Formatter;

#if defined(__cplusplus)
extern "C" {
#endif

/* Called by nc_initialize and nc_finalize respectively */
extern int NCZF_initialize(void);
extern int NCZF_finalize(void);

/* Wrappers for the formatter functions */

extern int NCZF_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
extern int NCZF_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
extern int NCZF_readmeta(NC_FILE_INFO_T* file);
extern int NCZF_writemeta(NC_FILE_INFO_T* file);
extern int NCZF_close(NC_FILE_INFO_T* file);
    
extern int NCZF_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, struct NCZ_AttrInfo**);

extern int NCZF_codec2hdf(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, struct NCZ_Plugin* plugin);
extern int NCZF_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter);

extern int NCZF_buildchunkkey(const NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);

/* Define known dispatch tables and initializers */
/* Each handles a specific NCZarr format + Pure Zarr */
/* WARNING: there is a lot of similar code in the dispatchers,
   so fixes to one may need to be propagated to the other dispatchers.
*/

extern const NCZ_Formatter* NCZ_formatter1; /* NCZarr V1 dispatch table => Zarr V2 */
extern const NCZ_Formatter* NCZ_formatter2; /* NCZarr V2 dispatch table => Zarr V2 */
extern const NCZ_Formatter* NCZ_formatter3; /* NCZarr V3 dispatch table => Zarr V3*/
/**************************************************/

/* Use inference to get map and the formatter */
extern int NCZ_get_map(NC_FILE_INFO_T* file, NCURI* url, int mode, size64_t constraints, void* params, NCZMAP** mapp);
extern int NCZ_get_formatter(NC_FILE_INFO_T* file, const NCZ_Formatter** formatterp);

#if defined(__cplusplus)
}
#endif

#endif /* ZFORMAT_H */
