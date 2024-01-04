/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#ifdef ENABLE_NCZARR_FILTERS
#include "zfilter.h"
#include "netcdf_filter_build.h"
#endif

/**************************************************/

/*Forward*/
static int ZF2_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_close(NC_FILE_INFO_T* file);
static int ZF2_writemeta(NC_FILE_INFO_T* file);
static int ZF2_readmeta(NC_FILE_INFO_T* file);
static int ZF2_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, struct NCZ_AttrInfo** ainfop);
static int ZF2_buildchunkkey(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);
#ifdef ENABLE_NCZARR_FILTERS
static int ZF2_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF2_codec2hdf(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, NCZ_Plugin* plugin);
#endif

static int write_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp);
static int write_var_meta(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);
static int write_var(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);
static int build_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_OBJ* container, NCindex* attlist, NCjson**, NCjson**);

static int read_superblock(NC_FILE_INFO_T* file, int* nczarrvp);
static int read_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
static int read_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo);
static int read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
static int read_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);

static int NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp);
static int NCZ_parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps);
static int NCZ_parse_group_content_pure(NCZ_FILE_INFO_T*  zinfo, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps);
static int NCZ_searchvars(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames);
static int NCZ_searchsubgrps(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* subgrpnames);
static int NCZ_decodeints(const NCjson* jshape, size64_t* shapes);
static int NCZ_computedimrefs(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_VAR_INFO_T* var, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);
static int NCZ_parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create);
static int NCZ_jtypes2atypes(int purezarr, const NCjson* jattrs, const NCjson* jtypes, nc_type** atypesp);
static int NCZ_locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int NCZ_createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp);

/**************************************************/

/**
 * @internal Synchronize file metadata from internal to map.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ZF2_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Internal->Map */

/**
 * @internal Synchronize file metadata from internal => map.
 * Disabled for V1.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error
 *	   ::NC_EXXX errors
 * @author Dennis Heimbigner
 */
static int
ZF2_writemeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    ZTRACE(4,"file=%s",file->controller->path);

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    map = zfile->map;
    assert(map != NULL);

    /* Write out root group recursively */
    if((stat = write_grp(file, zfile, map, file->root_grp)))
	goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Recursively synchronize group from memory to map.
 *
 * @param file Pointer to file struct
 * @param zfile
 * @param map
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp)
{
    int i,stat = NC_NOERR;
    char version[1024];
    int purezarr = 0;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrps = NULL;
    NCjson* jsuper = NULL;
    NCjson* jtmp = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;

    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    purezarr = (zfile->flags & FLAG_PUREZARR)?1:0;

    /* Construct grp key */
    if((stat = NCZ_grpkey(grp,&fullpath)))
	goto done;

    if(!purezarr) {
	/* Create dimensions dict */
	if((stat = NCZ_collect_dims(file,grp,&jdims))) goto done;

	/* Create vars list */
	NCJnew(NCJ_ARRAY,&jvars);
	for(i=0; i<ncindexsize(grp->vars); i++) {
	    NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	    NCJaddstring(jvars,NCJ_STRING,var->hdr.name);
	}

	/* Create subgroups list */
	NCJnew(NCJ_ARRAY,&jsubgrps);
	for(i=0; i<ncindexsize(grp->children); i++) {
	    NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	    NCJaddstring(jsubgrps,NCJ_STRING,g->hdr.name);
	}

	/* Create the "_nczarr_group" dict */
	NCJnew(NCJ_DICT,&json);
	/* Insert the various dicts and arrays */
	NCJinsert(json,"dims",jdims);
	jdims = NULL; /* avoid memory problems */
	NCJinsert(json,"vars",jvars);
	jvars = NULL; /* avoid memory problems */
	NCJinsert(json,"groups",jsubgrps);
	jsubgrps = NULL; /* avoid memory problems */
    }

    /* build ZGROUP contents */
    NCJcheck(NCJnew(NCJ_DICT,&jgroup));
    snprintf(version,sizeof(version),"%d",zfile->zarr.zarr_format);
    NCJcheck(NCJaddstring(jgroup,NCJ_STRING,"zarr_format"));
    NCJcheck(NCJaddstring(jgroup,NCJ_INT,version));
    if(!purezarr && grp->parent == NULL) { /* Root group */
	/* Track the library version that wrote this */
	strncpy(version,NCZARR_PACKAGE_VERSION,sizeof(version));
	NCJnew(NCJ_DICT,&jsuper);
	NCJnewstring(NCJ_STRING,version,&jtmp);
	NCJinsert(jsuper,"version",jtmp);
	jtmp = NULL;
	snprintf(version,sizeof(version),"%u", (unsigned)zfile->zarr.nczarr_format);
	NCJnewstring(NCJ_INT,version,&jtmp);
	NCJinsert(jsuper,"format",jtmp);
	jtmp = NULL;
	NCJinsert(jgroup,NCZ_V2_SUPERBLOCK,jsuper);
	jsuper = NULL;
    }

    if(!purezarr) {
	/* Insert the "_nczarr_group" dict */
	NCJinsert(jgroup,NCZ_V2_GROUP,json);
	json = NULL;
    }

    /* build Z2GROUP path */
    if((stat = nczm_concat(fullpath,Z2GROUP,&key)))
	goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jgroup)))
	goto done;
    nullfree(key); key = NULL;

    /* Build and write the Z2ATTRS object */
    assert(grp->att);
    if((stat = build_atts(file,zfile,map,(NC_OBJ*)grp, grp->att,&jatts,&jtypes)))goto done;
    /* write .zattrs path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jatts))) goto done;
    nullfree(key); key = NULL;

    /* Now write all the variables */
    for(i=0; i<ncindexsize(grp->vars); i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	if((stat = write_var(file,zfile,map,var))) goto done;
    }

    /* Now recurse to synchronize all the subgrps */
    for(i=0; i<ncindexsize(grp->children); i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = write_grp(file,zfile,map,g))) goto done;
    }

done:
    NCJreclaim(jtmp);
    NCJreclaim(jsuper);
    NCJreclaim(json);
    NCJreclaim(jgroup);
    NCJreclaim(jdims);
    NCJreclaim(jvars);
    NCJreclaim(jsubgrps);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_var_meta(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var)
{
    int i,stat = NC_NOERR;
     char number[1024];
     char* fullpath = NULL;
    char* key = NULL;
    char* dimpath = NULL;
    NClist* dimrefs = NULL;
    NCjson* jvar = NULL;
    NCjson* jncvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jtmp = NULL;
    NCjson* jfill = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    char* dtypename = NULL;
    int purezarr = 0;
    size64_t shape[NC_MAX_VAR_DIMS];
    NCZ_VAR_INFO_T* zvar = var->format_var_info;
#ifdef ENABLE_NCZARR_FILTERS
    NClist* filterchain = NULL;
    NCjson* jfilter = NULL;
#endif

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    zfile = file->format_file_info;
    map = zfile->map;

    purezarr = (zfile->flags & FLAG_PUREZARR)?1:0;

    /* Make sure that everything is established */
    /* ensure the fill value */
    if((stat = NCZ_ensure_fill_value(var))) goto done; /* ensure var->fill_value is set */
    assert(var->no_fill || var->fill_value != NULL);
    /* ensure the chunk cache */
    if((stat = NCZ_adjust_var_cache(var))) goto done;
    /* rebuild the fill chunk */
    if((stat = NCZ_ensure_fill_chunk(zvar->cache))) goto done;
#ifdef ENABLE_NCZARR_FILTERS
    /* Build the filter working parameters for any filters */
    if((stat = NCZ_filter_setup(var))) goto done;
#endif

    /* Construct var path */
    if((stat = NCZ_varkey(var,&fullpath)))
	goto done;

    /* Create the .zarray json object */
    NCJcheck(NCJnew(NCJ_DICT,&jvar));

    /* zarr_format key */
    snprintf(number,sizeof(number),"%d",zfile->zarr.zarr_format);
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"zarr_format"));
    NCJcheck(NCJaddstring(jvar,NCJ_INT,number));

    /* Collect the shape vector */
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	shape[i] = dim->len;
    }
    /* but might be scalar */
    if(var->ndims == 0)
	shape[0] = 1;

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    /* Create the list */
    NCJcheck(NCJnew(NCJ_ARRAY,&jtmp));
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1");
    } else for(i=0;i<var->ndims;i++) {
	snprintf(number,sizeof(number),"%llu",shape[i]);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    NCJcheck(NCJinsert(jvar,"shape",jtmp));
    jtmp = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"dtype"));
    {	/* Add the type name */
	int endianness = var->type_info->endianness;
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = ncz2_nctype2dtype(atomictype,endianness,purezarr,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename))) goto done;
	NCJaddstring(jvar,NCJ_STRING,dtypename);
	nullfree(dtypename); dtypename = NULL;
    }

    /* chunks key */
    /* The zarr format does not support the concept
       of contiguous (or compact), so it will never appear in the read case.
    */
    /* list of chunk sizes */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"chunks"));
    /* Create the list */
    NCJcheck(NCJnew(NCJ_ARRAY,&jtmp));
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1"); /* one chunk of size 1 */
    } else for(i=0;i<var->ndims;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(number,sizeof(number),"%lld",len);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    NCJcheck(NCJappend(jvar,jtmp));
    jtmp = NULL;

    /* fill_value key */
    if(var->no_fill) {
	NCJnew(NCJ_NULL,&jfill);
    } else {/*!var->no_fill*/
	int atomictype = var->type_info->hdr.id;
	if(var->fill_value == NULL) {
	     if((stat = NCZ_ensure_fill_value(var))) goto done;
	}
	/* Convert var->fill_value to a string */
	if((stat = NCZ_stringconvert(atomictype,1,var->fill_value,&jfill))) goto done;
	assert(jfill->sort != NCJ_ARRAY);
    }
    NCJcheck(NCJinsert(jvar,"fill_value",jfill));
    jfill = NULL;

    /* order key */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"order"));
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"C"));

    /* Compressor and Filters */
    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"compressor"));
#ifdef ENABLE_NCZARR_FILTERS
    filterchain = (NClist*)var->filters;
    if(nclistlength(filterchain) > 0) {
	struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,nclistlength(filterchain)-1);
	/* encode up the compressor */
	if((stat = NCZ_filter_jsonize(file,var,filter,&jtmp))) goto done;
    } else
#endif
    { /* no filters at all */
	/* Default to null */
	NCJnew(NCJ_NULL,&jtmp);
    }
    if(jtmp && NCJappend(jvar,jtmp)<0) goto done;
    jtmp = NULL;

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"filters"));
#ifdef ENABLE_NCZARR_FILTERS
    if(nclistlength(filterchain) > 1) {
	int k;
	/* jtmp holds the array of filters */
	NCJnew(NCJ_ARRAY,&jtmp);
	for(k=0;k<nclistlength(filterchain)-1;k++) {
	    struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,k);
	    /* encode up the filter as a string */
	    if((stat = NCZ_filter_jsonize(file,var,filter,&jfilter))) goto done;
	    NCJappend(jtmp,jfilter);
	}
    } else
#endif
    { /* no filters at all */
	NCJnew(NCJ_NULL,&jtmp);
    }
    NCJcheck(NCJappend(jvar,jtmp));
    jtmp = NULL;

    /* dimension_separator key */
    /* Single char defining the separator in chunk keys */
    if(zvar->dimension_separator != DFALT_DIM_SEPARATOR_V2) {
	char sep[2];
	sep[0] = zvar->dimension_separator;/* make separator a string*/
	sep[1] = '\0';
	NCJnewstring(NCJ_STRING,sep,&jtmp);
	NCJinsert(jvar,"dimension_separator",jtmp);
	jtmp = NULL;
    }

    /* Capture dimref names as FQNs */
    if(var->ndims > 0) {
	if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_dimkey(dim,&dimpath))) goto done;
	    nclistpush(dimrefs,dimpath);
	    dimpath = NULL;
	}
    }

    /* Build the NCZ_V2_ARRAY dict entry */
    {
	/* Create the dimrefs json object */
	NCJnew(NCJ_ARRAY,&jdimrefs);
	for(i=0;i<nclistlength(dimrefs);i++) {
	    const char* dim = nclistget(dimrefs,i);
	    NCJaddstring(jdimrefs,NCJ_STRING,dim);
	}
	NCJnew(NCJ_DICT,&jncvar);

	/* Insert dimrefs  */
	NCJinsert(jncvar,"dimrefs",jdimrefs);
	jdimrefs = NULL; /* Avoid memory problems */

	/* Add the _Storage flag */
	/* Record if this is a scalar */
	if(var->ndims == 0) {
	    NCJnewstring(NCJ_INT,"1",&jtmp);
	    NCJinsert(jncvar,"scalar",jtmp);
	    jtmp = NULL;
	}
	/* everything looks like it is chunked */
	NCJnewstring(NCJ_STRING,"chunked",&jtmp);
	NCJinsert(jncvar,"storage",jtmp);
	jtmp = NULL;

	if(!purezarr) {
	    NCJinsert(jvar,NCZ_V2_ARRAY,jncvar);
	    jncvar = NULL;
	}
    }

    /* build .zarray path */
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key)))
	goto done;

    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jvar)))
	goto done;
    nullfree(key); key = NULL;

    var->created = 1;

    /* Build and write .zattrs object */
    assert(var->att);
    if((stat = build_atts(file,zfile,map,(NC_OBJ*)var, var->att,&jatts,&jtypes)))goto done;
    /* write .zattrs path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nclistfreeall(dimrefs);
    nullfree(fullpath);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jncvar);
    NCJreclaim(jtmp);
    NCJreclaim(jfill);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data and data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_var(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    if((stat = write_var_meta(file,zfile,map,var))) goto done;

#if 0
    /* flush only chunks that have been written */
    if(zvar->cache) {
	if((stat = NCZ_flush_chunk_cache(zvar->cache)))
	    goto done;
    }
#endif

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize attribute data from memory to map.
 *
 * @param container Pointer to grp|var struct containing the attributes
 * @param key the name of the map entry
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
build_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_OBJ* container, NCindex* attlist, NCjson** jattsp, NCjson** jtypesp)
{
    int i,stat = NC_NOERR;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jtype = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jdict = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    char* content = NULL;
    char* dimpath = NULL;
    int isxarray = 0;
    int purezarr = 0;
    int inrootgroup = 0;
    NC_VAR_INFO_T* var = NULL;
    char* dtype = NULL;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    ZTRACE(3,"file=%s container=%s |attlist|=%u",file->controller->path,container->name,(unsigned)ncindexsize(attlist));

    purezarr = (zfile->flags & FLAG_PUREZARR)?1:0;
    if(zfile->flags & FLAG_XARRAYDIMS) isxarray = 1;

    if(container->sort == NCVAR) {
	var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL)
	    inrootgroup = 1;
    }

    /* Create the attribute dictionary */
    NCJcheck(NCJnew(NCJ_DICT,&jatts));

    if(!purezarr) {
  	/* Create the jncattr.types object */
	NCJnew(NCJ_DICT,&jtypes);
    }
    if(ncindexsize(attlist) > 0) {
	/* Walk all the attributes convert to json and collect the dtype */
	for(i=0;i<ncindexsize(attlist);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	    size_t typesize = 0;
	    nc_type internaltype = a->nc_typeid;
#if 0
	    const NC_reservedatt* ra = NC_findreserved(a->hdr.name);
	    /* If reserved and hidden, then ignore */
	    if(ra && (ra->flags & HIDDENATTRFLAG)) continue;
#endif
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
		typesize = NCZ_get_maxstrlen(container);
	    else
		{if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}

  	    /* Track complex json representation*/
            if(internaltype == NC_CHAR && NCZ_iscomplexjsontext(a->len,(char*)a->data,&jdata)) {
		internaltype = NC_JSON; /* hack to remember this case */
		typesize = 0;
	    } else {
	        if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;
	    }
	    NCJinsert(jatts,a->hdr.name,jdata);
	    jdata = NULL;

	    if(!purezarr) {
	        /* Collect the corresponding dtype */
		if((stat = ncz2_nctype2dtype(internaltype,endianness,purezarr,typesize,&dtype))) goto done;
		NCJnewstring(NCJ_STRING,dtype,&jtype);
		nullfree(dtype); dtype = NULL;
		NCJinsert(jtypes,a->hdr.name,jtype); /* add {name: type} */
		jtype = NULL;
	    }
	}
    }

#if 0
    /* Construct container path */
    if(container->sort == NCGRP)
	stat = NCZ_grpkey(grp,&fullpath);
    else
	stat = NCZ_varkey(var,&fullpath);
    if(stat)
	goto done;
#endif

    /* Add Quantize Attribute */
    if(container->sort == NCVAR && var && var->quantize_mode > 0) {
        char mode[64];
        const char* qattname = NULL;
        snprintf(mode,sizeof(mode),"%d",var->nsd);
        NCJcheck(NCJnewstring(NCJ_INT,mode,&jint));
        /* Insert the quantize attribute */
        switch (var->quantize_mode) {
        case NC_QUANTIZE_BITGROOM:
            qattname = NC_QUANTIZE_BITGROOM_ATT_NAME;
            break;
        case NC_QUANTIZE_GRANULARBR:
            qattname = NC_QUANTIZE_GRANULARBR_ATT_NAME;
            break;
        case NC_QUANTIZE_BITROUND:
            qattname = NC_QUANTIZE_BITROUND_ATT_NAME;
            break;
        default: {stat = NC_ENCZARR; goto done;}
        }
        if(!purezarr) {
            NCJcheck(NCJnewstring(NCJ_STRING,"<u4",&jtype));
            NCJcheck(NCJinsert(jtypes,qattname,jtype));
            jtype = NULL;
	    }
        NCJcheck(NCJinsert(jatts,qattname,jint));
        jint = NULL;
    }

    /* Insert option XARRAY attribute */
    if(container->sort == NCVAR) {
	if(inrootgroup && isxarray) {
	    int dimsinroot = 1;
	    /* Insert the XARRAY _ARRAY_ATTRIBUTE attribute */
	    NCJnew(NCJ_ARRAY,&jdimrefs);
	    /* Fake the scalar case */
	    if(var->ndims == 0) {
		NCJaddstring(jdimrefs,NCJ_STRING,XARRAYSCALAR);
		dimsinroot = 1; /* define XARRAYSCALAR in root group */
	    } else { /* Walk the dimensions and capture the names */
	        for(i=0;i<var->ndims;i++) {
		    NC_DIM_INFO_T* dim = var->dim[i];
		    /* Verify that the dimension is in the root group */
		    if(dim->container && dim->container->parent != NULL) {
		        dimsinroot = 0; /* dimension is not in root */
		        break;
		    }
	        }
	    }
            if(dimsinroot) {
                /* Walk the dimensions and capture the names */
                for(i=0;i<var->ndims;i++) {
                    char* dimname;
                    NC_DIM_INFO_T* dim = var->dim[i];
                    dimname = strdup(dim->hdr.name);
                    if(dimname == NULL) {stat = NC_ENOMEM; goto done;}
                    NCJcheck(NCJaddstring(jdimrefs,NCJ_STRING,dimname));
                    nullfree(dimname); dimname = NULL;
                }
                /* Add the _ARRAY_DIMENSIONS attribute */
                NCJcheck(NCJinsert(jatts,NC_XARRAY_DIMS,jdimrefs));
                jdimrefs = NULL;
                /* And a fake type */
                if(!purezarr) {
                    NCJcheck(NCJnewstring(NCJ_STRING,"|J0",&jtype));
                    NCJcheck(NCJinsert(jtypes,NC_XARRAY_DIMS,jtype)); /* add {name: type} */
                    jtype = NULL;
                }
            }
        }
    }
    if(NCJdictlength(jatts) > 0) {
        if(!purezarr) {
            /* Insert the NCZ_V2_ATTR attribute */
            /* Add a type */
	    assert(jtype == NULL);
            NCJcheck(NCJnewstring(NCJ_STRING,">S1",&jtype));
            NCJcheck(NCJinsert(jtypes,NCZ_V2_ATTR,jtype));            
            jtype = NULL;
            NCJcheck(NCJnew(NCJ_DICT,&jdict));
            if(jtypes != NULL)
               NCJcheck(NCJinsert(jdict,"types",jtypes));
            jtypes = NULL;
            if(jdict != NULL)
                NCJcheck(NCJinsert(jatts,NCZ_V2_ATTR,jdict));
            jdict = NULL;
 	}
    }

    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jtypesp) {*jtypesp = jtypes; jtypes = NULL;}
done:
    nullfree(fullpath);
    nullfree(key);
    nullfree(content);
    nullfree(dimpath);
    nullfree(dtype);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
    NCJreclaim(jtype);
    NCJreclaim(jdimrefs);
    NCJreclaim(jdict);
    NCJreclaim(jint);
    NCJreclaim(jdata);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/

/**************************************************/
/* Map->Internal */


/**
 * @internal Read file data from map to memory.
 *
 * @param file Pointer to file info struct.
 *
 * @return NC_NOERR If no error.
 *         NC_EXXX for error returns
 *
 * @author Dennis Heimbigner
 */
static int
ZF2_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    int purezarr = 0;
    int nczarr_format = 0;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(3,"file=%s",file->controller->path);

    zfile = file->format_file_info;

    purezarr = (zfile->flags & FLAG_PUREZARR);

    /* Ok, try to read superblock */
    switch(stat = read_superblock(file,&nczarr_format)) {
    case NC_NOERR: break;
    case NC_EEMPTY:
        if(!purezarr) {stat = NC_ENOTZARR; goto done;}
        stat = NC_NOERR;
        break;
    default: goto done;
    }

    /* Now load the groups starting with root */
    if((stat = read_grp(file,file->root_grp)))
        goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Create attributes from info stored in NCZ_{GRP|VAR}_INFO_T object.
@param file - [in] the containing file (annotation)
@param container - [in] the containing object (var or grp) into which to store the attributes
@param ainfop - [out] the standardized attribute info.
@return NC_NOERR|NC_EXXX

@author Dennis Heimbigner
*/
static int
ZF2_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, struct NCZ_AttrInfo** ainfop)
{
    int stat = NC_NOERR;
    const char* fullpath = NULL;
    char* key = NULL;
    NCjson* jattrs = NULL;
    const NCjson* jncattr = NULL;
    const NCjson* jtypes = NULL;
    nc_type* atypes = NULL;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    int purezarr = 0;
    struct NCZ_AttrInfo* ainfo = NULL;
    size_t natts = 0;
    size_t i;

    assert(jatts == NULL);
    
    ZTRACE(3,"map=%p container=%s nczarrv1=%d",map,container->name,nczarrv1);

    purezarr = (zfile->flags & FLAG_PUREZARR)?1:0;

    if(container->sort == NCGRP) {
        NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)container;
        NCZ_GRP_INFO_T* zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;
        /* Get grp's fullpath name */
        assert(zgrp->grppath != NULL);
        fullpath = zgrp->grppath;
    } else {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)container;
        NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
        /* Get var's fullpath name */
        assert(zvar->varpath != NULL);
        fullpath = zvar->varpath;
    }

    /* Construct the path to the .zattrs object */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key)))
        goto done;

    /* Download the .zattrs object: may not exist */
    switch ((stat=NCZ_downloadjson(zfile->map,key,&jattrs))) {
    case NC_NOERR: break;
    case NC_EEMPTY: stat = NC_NOERR; break; /* did not exist */
    default: goto done; /* failure */
    }
    nullfree(key); key = NULL;

    if(jattrs != NULL) {
        if(NCJsort(jattrs) != NCJ_DICT) {stat = THROW(NC_ENCZARR); goto done;}
        natts = NCJdictlength(jattrs);
        /* Get _nczarr_attrs from .zattrs (may be null)*/
        NCJcheck(NCJdictget(jattrs,NCZ_V2_ATTR,&jncattr));
        nullfree(key); key = NULL;
        if(jncattr != NULL) {
            /* jncattr attribute should be a dict */
            if(NCJsort(jncattr) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
            /* Extract "types; may not exist if purezarr or only hidden attributes are defined */
            NCJcheck(NCJdictget(jncattr,"types",&jtypes));
        }
        /* Convert to a vector of nc_types */ 
        if((stat = NCZ_jtypes2atypes(purezarr, jattrs, jtypes, &atypes))) goto done;

        /* Fill in the ainfo; assumes jatts sorted and atypes order corresponds */
        {
            if((ainfo = (struct NCZ_AttrInfo*)calloc(natts+1,sizeof(struct NCZ_AttrInfo)))==NULL) {stat = NC_ENOMEM; goto done;}
            for(i=0;i<natts;i++) {
                NCjson* jkey = NULL;
                NCjson* jvalues = NULL;
                jkey = NCJdictkey(jattrs,i);
                assert(jkey != NULL && NCJisatomic(jkey));
                ainfo[i].name = strdup(NCJstring(jkey));
                ainfo[i].nctype = atypes[i];
                jvalues = NCJdictvalue(jattrs,i);
		assert(ainfo[i].values == NULL);
                if((stat= NCJclone(jvalues,(NCjson**)&ainfo[i].values))) goto done;
            }
        }
    }
    if(ainfop) {*ainfop = ainfo; ainfo = NULL;}

done:
    NCJreclaim(jattrs);
    nullfree(atypes);
    NCZ_freeAttrInfoVec(ainfo);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/
/**
 * @internal Read superblock data from map to memory
 *
 * @param file Pointer to file struct
 * @param nczarrvp (out) the nczarr version
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_superblock(NC_FILE_INFO_T* file, int* nczarrvp)
{
    int stat = NC_NOERR;
    int nczarr_format = 0;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jblock = NULL;
    const NCjson* jtmp = NULL;
    const NCjson* jtmp2 = NULL;

    ZTRACE(3,"file=%s",file->controller->path);

    /* Get the Zarr root group */
    switch(stat = NCZ_downloadjson(zfile->map, Z2METAROOT, &jblock)) {
    case NC_EEMPTY: /* not there */
        nczarr_format = NCZARRFORMAT0; /* apparently pure zarr */
        zfile->flags |= FLAG_PUREZARR;
        stat = NC_NOERR; /* reset */
        goto done;
    case NC_NOERR:
        /* See if _nczarr_superblock key exists */
        NCJcheck(NCJdictget(jblock,NCZ_V2_SUPERBLOCK,&jtmp));
        if(jtmp != NULL) {
            /* in any case this is nczarr format 2 */
            nczarr_format = 2;
            /* See if superblock has version and (optionally) format */
            NCJcheck(NCJdictget(jtmp,"version",&jtmp2));
            if(jtmp2 == NULL) {stat = NC_ENCZARR; goto done;} /* Malformed */
            NCJcheck(NCJdictget(jtmp,"format",&jtmp2));
            if(jtmp2 != NULL)
                sscanf(NCJstring(jtmp2),"%d",&nczarr_format);
        }
        break;
    default: goto done;
    }
    if(nczarrvp) *nczarrvp = nczarr_format;
done:
    NCJreclaim(jblock);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Read group data from map to memory
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    char* fullpath = NULL;
    char* key = NULL;
    int purezarr = 0;
    NCjson* json = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdict = NULL;
    NClist* dimdefs = nclistnew();
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_GRP_INFO_T* zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;
    
    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    purezarr = (zfile->flags & FLAG_PUREZARR);

    /* Construct grp path and stash a copy*/
    if((stat = NCZ_grpkey(grp,&fullpath))) goto done;
    zgrp->grppath = strdup(fullpath);

    if(purezarr) {
        if((stat = NCZ_parse_group_content_pure(zfile,grp,varnames,subgrps)))
            goto done;
    } else { /*!purezarr*/
        /* build Z2METAROOT path */
        if((stat = nczm_concat(fullpath,Z2METAROOT,&key))) goto done;
        /* Read */
        jdict = NULL;
        stat=NCZ_downloadjson(zfile->map,key,&jdict);
        nullfree(key); key = NULL;
        if(!jdict) {stat = NC_ENOTZARR; goto done;}
        /* Pull out lists about group content */
        if((stat = NCZ_parse_group_content(jdict,dimdefs,varnames,subgrps))) goto done;
        /* Define dimensions */
        if((stat = read_dims(file,grp,dimdefs))) goto done;
    }

    /* Define vars taking xarray into account */
    if((stat = read_vars(file,grp,varnames))) goto done;

    /* Read sub-groups */
    if((stat = read_subgrps(file,grp,subgrps))) goto done;

done:
    NCJreclaim(jdict);
    NCJreclaim(json);
    NCJreclaim(jgroup);
    nclistfreeall(dimdefs);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param diminfo List of (name,length,isunlimited) triples
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo)
{
    int i,stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |diminfo|=%u",file->controller->path,grp->hdr.name,nclistlength(diminfo));

    /* Reify each dim in turn */
    for(i = 0; i < nclistlength(diminfo); i+=3) {
        NC_DIM_INFO_T* dim = NULL;
        size64_t len = 0;
        long long isunlim = 0;
        const char* name = nclistget(diminfo,i);
        const char* slen = nclistget(diminfo,i+1);
        const char* sisunlimited = nclistget(diminfo,i+2);

        /* Create the NC_DIM_INFO_T object */
        sscanf(slen,"%lld",&len); /* Get length */
        if(sisunlimited != NULL)
            sscanf(sisunlimited,"%lld",&isunlim); /* Get unlimited flag */
        else
            isunlim = 0;
        if((stat = nc4_dim_list_add(grp, name, (size_t)len, -1, &dim)))
            goto done;
        dim->unlimited = (isunlim ? 1 : 0);
        if((dim->format_dim_info = calloc(1,sizeof(NCZ_DIM_INFO_T))) == NULL)
            {stat = NC_ENOMEM; goto done;}
        ((NCZ_DIM_INFO_T*)dim->format_dim_info)->common.file = file;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize a single var into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param zfile Zarr specific file info
 * @param map Zarr storage map handler
 * @param grp Pointer to grp info struct.
 * @param var variable name
 * @param suppressp flat to suppress variable
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname)
{
    int stat = NC_NOERR;
    int j;
    int purezarr = 0;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NCjson* jvar = NULL;
    const NCjson* jncvar = NULL;
    const NCjson* jdimrefs = NULL;
    const NCjson* jvalue = NULL;
    char* varpath = NULL;
    char* key = NULL;
    size64_t* shapes = NULL;
    NClist* dimnames = NULL;
    int suppress = 0; /* Abort processing of this variable */
    nc_type vtype = NC_NAT;
    size_t vtypelen = 0;
    int rank = 0;
    int zarr_rank = 0; /* Need to watch out for scalars */
#ifdef ENABLE_NCZARR_FILTERS
    int varsized = 0;
    const NCjson* jfilter = NULL;
    int chainindex = 0;
#endif

    purezarr = (zfile->flags & FLAG_PUREZARR)?1:0;
    dimnames = nclistnew();

    if((stat = nc4_var_list_add2(grp, varname, &var))) goto done;

    /* Set var annotation */
    if((zvar = calloc(1,sizeof(NCZ_VAR_INFO_T)))==NULL) {stat = NC_ENOMEM; goto done;}
    var->format_var_info = zvar;
    zvar->common.file = file;

    /* pretend it was created */
    var->created = 1;

    /* Indicate we do not have quantizer yet */
    var->quantize_mode = -1;

    /* Construct var path and stash a copy */
    if((stat = NCZ_varkey(var,&varpath))) goto done;
    zvar->varpath = strdup(varpath);

    /* Construct the path to the zarray object */
    if((stat = nczm_concat(varpath,Z2ARRAY,&key))) goto done;
    /* Download the zarray object */
    if((stat=NCZ_readdict(zfile->map,key,&jvar))) goto done;
    nullfree(key); key = NULL;
    assert(NCJsort(jvar) == NCJ_DICT);

    /* Extract the .zarray info from jvar */

    /* Verify the format */
    {
        int version;
        NCJcheck(NCJdictget(jvar,"zarr_format",&jvalue));
        sscanf(NCJstring(jvalue),"%d",&version);
        if(version != zfile->zarr.zarr_format)
            {stat = (THROW(NC_ENCZARR)); goto done;}
    }

    /* Set the type and endianness of the variable */
    {
        int endianness;
        NCJcheck(NCJdictget(jvar,"dtype",&jvalue));
        /* Convert dtype to nc_type + endianness */
        if((stat = ncz2_dtype2nctype(NCJstring(jvalue),NC_NAT,purezarr,&vtype,&endianness,&vtypelen)))
            goto done;
        if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
            /* Locate the NC_TYPE_INFO_T object */
            if((stat = ncz_gettype(file,grp,vtype,&var->type_info)))
                goto done;
        } else {stat = NC_EBADTYPE; goto done;}
        var->endianness = endianness;
        var->type_info->endianness = var->endianness; /* Propagate */
        if(vtype == NC_STRING) {
            zvar->maxstrlen = vtypelen;
            vtypelen = sizeof(char*); /* in-memory len */
            if(zvar->maxstrlen <= 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
        }
    }

    if(!purezarr) {
        /* Extract the _NCZARR_ARRAY values */
        /* Do this first so we know about storage esp. scalar */
        /* Extract the NCZ_V2_ARRAY dict */
        NCJcheck(NCJdictget(jvar,NCZ_V2_ARRAY,&jncvar));
        if(!stat && jncvar == NULL)
            NCJcheck(NCJdictget(jvar,NCZ_V2_ARRAY,&jncvar));
        if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
        assert((NCJsort(jncvar) == NCJ_DICT));
        /* Extract scalar flag */
        NCJcheck(NCJdictget(jncvar,"scalar",&jvalue));
        if(jvalue != NULL) {
            var->storage = NC_CHUNKED;
            zvar->scalar = 1;
        }
        /* Extract storage flag */
        NCJcheck(NCJdictget(jncvar,"storage",&jvalue));
        if(jvalue != NULL) {
            var->storage = NC_CHUNKED;
        }
        /* Extract dimrefs list  */
        NCJcheck(NCJdictget(jncvar,"dimrefs",&jdimrefs));
	if(jdimrefs != NULL) {
            /* Extract the dimref names */
            assert((NCJsort(jdimrefs) == NCJ_ARRAY));
            if(zvar->scalar) {
                assert(NCJarraylength(jdimrefs) == 0);
            } else {
                rank = NCJarraylength(jdimrefs);
                for(j=0;j<rank;j++) {
                    const NCjson* dimpath = NCJith(jdimrefs,j);
                    assert(NCJisatomic(dimpath));
                    nclistpush(dimnames,strdup(NCJstring(dimpath)));
                }
            }
            jdimrefs = NULL; /* avoid double free */
	} else { /* will simulate it from the shape of the variable */
            stat = NC_NOERR;
        }
        jdimrefs = NULL;
    }

    /* Capture dimension_separator (must precede chunk cache creation) */
    {
        NCglobalstate* ngs = NC_getglobalstate();
        assert(ngs != NULL);
        zvar->dimension_separator = 0;
        NCJcheck(NCJdictget(jvar,"dimension_separator",&jvalue));
        if(jvalue != NULL) {
            /* Verify its value */
            if(NCJisatomic(jvalue) && NCJstring(jvalue) != NULL && strlen(NCJstring(jvalue)) == 1)
               zvar->dimension_separator = NCJstring(jvalue)[0];
        }
        /* If value is invalid, then use global default */
        if(!islegaldimsep(zvar->dimension_separator))
            zvar->dimension_separator = ngs->zarr.dimension_separator; /* use global value */
        assert(islegaldimsep(zvar->dimension_separator)); /* we are hosed */
    }

    /* fill_value; must precede calls to adjust cache */
    {
        NCJcheck(NCJdictget(jvar,"fill_value",&jvalue));
        if(jvalue == NULL || NCJsort(jvalue) == NCJ_NULL)
            var->no_fill = 1;
        else {
            size_t fvlen;
            nc_type atypeid = vtype;
            var->no_fill = 0;
            if((stat = NCZ_computeattrdata(var->type_info->hdr.id, &atypeid, jvalue, NULL, &fvlen, &var->fill_value)))
                goto done;
            assert(atypeid == vtype);
            /* Note that we do not create the _FillValue
               attribute here to avoid having to read all
               the attributes and thus foiling lazy read.*/
        }
    }

    /* shape */
    {
        NCJcheck(NCJdictget(jvar,"shape",&jvalue));
        if(NCJsort(jvalue) != NCJ_ARRAY) {stat = (THROW(NC_ENCZARR)); goto done;}

        /* Process the rank */
        zarr_rank = NCJarraylength(jvalue);
        if(zarr_rank == 0) {
            /* suppress variable */
            ZLOG(NCLOGWARN,"Empty shape for variable %s suppressed",var->hdr.name);
            suppress = 1;
            goto suppressvar;
        }

        if(zvar->scalar) {
            rank = 0;
            zarr_rank = 1; /* Zarr does not support scalars */
        } else
            rank = (zarr_rank = NCJarraylength(jvalue));

        if(zarr_rank > 0) {
            /* Save the rank of the variable */
            if((stat = nc4_var_set_ndims(var, rank))) goto done;
            /* extract the shapes */
            if((shapes = (size64_t*)malloc(sizeof(size64_t)*zarr_rank)) == NULL)
                {stat = (THROW(NC_ENOMEM)); goto done;}
            if((stat = NCZ_decodeints(jvalue, shapes))) goto done;
        }
    }

    /* chunks */
    {
        size64_t chunks[NC_MAX_VAR_DIMS];
        NCJcheck(NCJdictget(jvar,"chunks",&jvalue));
        if(jvalue != NULL && NCJsort(jvalue) != NCJ_ARRAY)
            {stat = (THROW(NC_ENCZARR)); goto done;}
        /* Verify the rank */
        if(zvar->scalar || zarr_rank == 0) {
            if(var->ndims != 0)
                {stat = (THROW(NC_ENCZARR)); goto done;}
            zvar->chunkproduct = 1;
            zvar->chunksize = zvar->chunkproduct * var->type_info->size;
            /* Create the cache */
            if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
                goto done;
        } else {/* !zvar->scalar */
            if(zarr_rank == 0) {stat = NC_ENCZARR; goto done;}
            var->storage = NC_CHUNKED;
            if(var->ndims != rank)
                {stat = (THROW(NC_ENCZARR)); goto done;}
            if((var->chunksizes = malloc(sizeof(size_t)*zarr_rank)) == NULL)
                {stat = NC_ENOMEM; goto done;}
            if((stat = NCZ_decodeints(jvalue, chunks))) goto done;
            /* validate the chunk sizes */
            zvar->chunkproduct = 1;
            for(j=0;j<rank;j++) {
                if(chunks[j] == 0)
                    {stat = (THROW(NC_ENCZARR)); goto done;}
                var->chunksizes[j] = (size_t)chunks[j];
                zvar->chunkproduct *= chunks[j];
            }
            zvar->chunksize = zvar->chunkproduct * var->type_info->size;
            /* Create the cache */
            if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
                goto done;
        }
        if((stat = NCZ_adjust_var_cache(var))) goto done;
    }

    /* Capture row vs column major; currently, column major not used*/
    {
        NCJcheck(NCJdictget(jvar,"order",&jvalue));
        if(strcmp(NCJstring(jvalue),"C")==1)
            ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 1;
        else ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 0;
    }

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* Do filters key before compressor key so final filter chain is in correct order */
    {
#ifdef ENABLE_NCZARR_FILTERS
        if(var->filters == NULL) var->filters = (void*)nclistnew();
        if(zvar->incompletefilters == NULL) zvar->incompletefilters = (void*)nclistnew();
        chainindex = 0; /* track location of filter in the chain */
        if((stat = NCZ_filter_initialize())) goto done;
        NCJcheck(NCJdictget(jvar,"filters",&jvalue));
        if(jvalue != NULL && NCJsort(jvalue) != NCJ_NULL) {
            int k;
            if(NCJsort(jvalue) != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
            for(k=0;;k++) {
                jfilter = NULL;
                jfilter = NCJith(jvalue,k);
                if(jfilter == NULL) break; /* done */
                if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
                if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
            }
        }
#endif
    }

    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
#ifdef ENABLE_NCZARR_FILTERS
    {
        if(var->filters == NULL) var->filters = (void*)nclistnew();
        if((stat = NCZ_filter_initialize())) goto done;
        NCJcheck(NCJdictget(jvar,"compressor",&jfilter));
        if(jfilter != NULL && NCJsort(jfilter) != NCJ_NULL) {
            if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
            if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
        }
    }
    /* Suppress variable if there are filters and var is not fixed-size */
    if(varsized && nclistlength((NClist*)var->filters) > 0)
        suppress = 1;
#endif

    if(zarr_rank > 0) {
        if((stat = NCZ_computedimrefs(file, zfile, zfile->map, var, rank, dimnames, shapes, var->dim)))
            goto done;
        if(!zvar->scalar) {
            /* Extract the dimids */
            for(j=0;j<rank;j++)
                var->dimids[j] = var->dim[j]->hdr.id;
        }
    }

#ifdef ENABLE_NCZARR_FILTERS
    if(!suppress) {
        /* At this point, we can finalize the filters */
        if((stat = NCZ_filter_setup(var))) goto done;
    }
#endif

suppressvar:
    if(suppress) {
        /* Reclaim NCZarr variable specific info */
        (void)NCZ_zclose_var1(var);
        /* Remove from list of variables and reclaim the top level var object */
        (void)nc4_var_list_del(grp, var);
        var = NULL;
    }

done:
    /* Clean up */
    nclistfreeall(dimnames);
    nullfree(varpath);
    nullfree(shapes);
    nullfree(key);
    NCJreclaim(jvar);

    return THROW(stat);
}

/**
 * @internal Materialize vars into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varnames List of names of variables in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    int i;

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
        const char* varname = (const char*)nclistget(varnames,i);
        if((stat = read_var1(file,grp,varname))) goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize subgroups into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param subgrpnames List of names of subgroups in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int i,stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |subgrpnames|=%u",file->controller->path,grp->hdr.name,nclistlength(subgrpnames));

    /* Load each subgroup name in turn */
    for(i = 0; i < nclistlength(subgrpnames); i++) {
        NC_GRP_INFO_T* g = NULL;
        const char* gname = nclistget(subgrpnames,i);
        char norm_name[NC_MAX_NAME];
        /* Check and normalize the name. */
        if((stat = nc4_check_name(gname, norm_name)))
            goto done;
        if((stat = nc4_grp_list_add(file, grp, norm_name, &g)))
            goto done;
        if(!(g->format_grp_info = calloc(1, sizeof(NCZ_GRP_INFO_T))))
            {stat = NC_ENOMEM; goto done;}
        ((NCZ_GRP_INFO_T*)g->format_grp_info)->common.file = file;
    }

    /* Recurse to fill in subgroups */
    for(i=0;i<ncindexsize(grp->children);i++) {
        NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
        if((stat = read_grp(file,g))) goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Potentially shared functions */

/**
 * @internal Synchronize dimension data from memory to map.
 *
 * @param grp Pointer to grp struct containing the dims.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp)
{
    int i, stat=NC_NOERR;
    NCjson* jdims = NULL;
    NCjson* jdimsize = NULL;
    NCjson* jdimargs = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJcheck(NCJnew(NCJ_DICT,&jdims));
    for(i=0; i<ncindexsize(grp->dim); i++) {
        NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
        char slen[128];

        snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
        NCJcheck(NCJnewstring(NCJ_INT,slen,&jdimsize));

        /* If dim is not unlimited, then write in the old format to provide
           maximum back compatibility.
        */
        if(dim->unlimited) {
            NCJcheck(NCJnew(NCJ_DICT,&jdimargs));
            NCJcheck(NCJaddstring(jdimargs,NCJ_STRING,"size"));
            NCJcheck(NCJappend(jdimargs,jdimsize));
            jdimsize = NULL;
            NCJcheck(NCJaddstring(jdimargs,NCJ_STRING,"unlimited"));
            NCJcheck(NCJaddstring(jdimargs,NCJ_INT,"1"));
        } else { /* !dim->unlimited */
            jdimargs = jdimsize;
            jdimsize = NULL;
        }
        NCJcheck(NCJaddstring(jdims,NCJ_STRING,dim->hdr.name));
        NCJcheck(NCJappend(jdims,jdimargs));
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}

    NCJreclaim(jdims);
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps)
{
    int i,stat = NC_NOERR;
    const NCjson* jvalue = NULL;
    const NCjson* jgrp = NULL;


    ZTRACE(3,"jcontent=|%s| |dimdefs|=%u |varnames|=%u |subgrps|=%u",NCJtotext(jcontent),(unsigned)nclistlength(dimdefs),(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    /* Get the _nczarr_group key */
    NCJcheck(NCJdictget(jcontent,NCZ_V2_GROUP,&jgrp));
    if(jgrp == NULL) {stat = NC_ENCZARR; goto done;}

    /* Now get nczarr specific keys */
    NCJcheck(NCJdictget(jgrp,"dims",&jvalue));
    if(jvalue != NULL) {
        if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
        /* Extract the dimensions defined in this group */
        for(i=0;i<NCJdictlength(jvalue);i++) {
            const NCjson* jname = NCJdictkey(jvalue,i);
            const NCjson* jleninfo = NCJdictvalue(jvalue,i);
            const NCjson* jtmp = NULL;
            const char* slen = "0";
            const char* sunlim = "0";
            char norm_name[NC_MAX_NAME + 1];
            /* Verify name legality */
            if((stat = nc4_check_name(NCJstring(jname), norm_name)))
                {stat = NC_EBADNAME; goto done;}
            /* check the length */
            if(NCJsort(jleninfo) == NCJ_DICT) {
                NCJcheck(NCJdictget(jleninfo,"size",&jtmp));
                if(jtmp== NULL)
                    {stat = NC_EBADNAME; goto done;}
                slen = NCJstring(jtmp);
                /* See if unlimited */
                NCJcheck(NCJdictget(jleninfo,"unlimited",&jtmp));
                if(jtmp == NULL) sunlim = "0"; else sunlim = NCJstring(jtmp);
            } else if(jleninfo != NULL && NCJsort(jleninfo) == NCJ_INT) {
                slen = NCJstring(jleninfo);
            } else
                {stat = NC_ENCZARR; goto done;}
            nclistpush(dimdefs,strdup(norm_name));
            nclistpush(dimdefs,strdup(slen));
            nclistpush(dimdefs,strdup(sunlim));
        }
    }

    NCJcheck(NCJdictget(jgrp,"vars",&jvalue));
    if(jvalue != NULL) {
        /* Extract the variable names in this group */
        for(i=0;i<NCJarraylength(jvalue);i++) {
            NCjson* jname = NCJith(jvalue,i);
            char norm_name[NC_MAX_NAME + 1];
            /* Verify name legality */
            if((stat = nc4_check_name(NCJstring(jname), norm_name)))
                {stat = NC_EBADNAME; goto done;}
            nclistpush(varnames,strdup(norm_name));
        }
    }

    NCJcheck(NCJdictget(jgrp,"groups",&jvalue));
    if(jvalue != NULL) {
        /* Extract the subgroup names in this group */
        for(i=0;i<NCJarraylength(jvalue);i++) {
            NCjson* jname = NCJith(jvalue,i);
            char norm_name[NC_MAX_NAME + 1];
            /* Verify name legality */
            if((stat = nc4_check_name(NCJstring(jname), norm_name)))
                {stat = NC_EBADNAME; goto done;}
            nclistpush(subgrps,strdup(norm_name));
        }
    }

done:
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_parse_group_content_pure(NCZ_FILE_INFO_T*  zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;

    ZTRACE(3,"zfile=%s grp=%s |varnames|=%u |subgrps|=%u",zfile->common.file->controller->path,grp->hdr.name,(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    nclistclear(varnames);
    if((stat = NCZ_searchvars(zfile,grp,varnames))) goto done;
    nclistclear(subgrps);
    if((stat = NCZ_searchsubgrps(zfile,grp,subgrps))) goto done;

done:
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_searchvars(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int i,stat = NC_NOERR;
    char* grpkey = NULL;
    char* varkey = NULL;
    char* zarray = NULL;
    NClist* matches = nclistnew();

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_list(zfile->map,grpkey,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
        const char* name = nclistget(matches,i);
        if(name[0] == NCZM_DOT) continue; /* zarr/nczarr specific */
        /* See if name/.zarray exists */
        if((stat = nczm_concat(grpkey,name,&varkey))) goto done;
        if((stat = nczm_concat(varkey,Z2ARRAY,&zarray))) goto done;
        if((stat = nczmap_exists(zfile->map,zarray)) == NC_NOERR)
            nclistpush(varnames,strdup(name));
        stat = NC_NOERR;
        nullfree(varkey); varkey = NULL;
        nullfree(zarray); zarray = NULL;
    }

done:
    nullfree(grpkey);
    nullfree(varkey);
    nullfree(zarray);
    nclistfreeall(matches);
    return stat;
}

static int
NCZ_searchsubgrps(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int i,stat = NC_NOERR;
    char* grpkey = NULL;
    char* subkey = NULL;
    char* zgroup = NULL;
    NClist* matches = nclistnew();

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_list(zfile->map,grpkey,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
        const char* name = nclistget(matches,i);
        if(name[0] == NCZM_DOT) continue; /* zarr/nczarr specific */
        /* See if name/.zgroup exists */
        if((stat = nczm_concat(grpkey,name,&subkey))) goto done;
        if((stat = nczm_concat(subkey,Z2GROUP,&zgroup))) goto done;
        if((stat = nczmap_exists(zfile->map,zgroup)) == NC_NOERR)
            nclistpush(subgrpnames,strdup(name));
        stat = NC_NOERR;
        nullfree(subkey); subkey = NULL;
        nullfree(zgroup); zgroup = NULL;
    }

done:
    nullfree(grpkey);
    nullfree(subkey);
    nullfree(zgroup);
    nclistfreeall(matches);
    return stat;
}

/* Convert a list of integer strings to 64 bit dimension sizes (shapes) */
static int
NCZ_decodeints(const NCjson* jshape, size64_t* shapes)
{
    int i, stat = NC_NOERR;

    for(i=0;i<NCJarraylength(jshape);i++) {
        struct ZCVT zcvt;
        nc_type typeid = NC_NAT;
        NCjson* jv = NCJith(jshape,i);
        if((stat = NCZ_json2cvt(jv,&zcvt,&typeid))) goto done;
        switch (typeid) {
        case NC_INT64:
        if(zcvt.int64v < 0) {stat = (THROW(NC_ENCZARR)); goto done;}
            shapes[i] = (size64_t)zcvt.int64v;
            break;
        case NC_UINT64:
            shapes[i] = (size64_t)zcvt.uint64v;
            break;
        default: {stat = (THROW(NC_ENCZARR)); goto done;}
        }
    }

done:
    return THROW(stat);
}

/* Compute the set of dim refs for this variable, taking purezarr and xarray into account */
static int
NCZ_computedimrefs(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int stat = NC_NOERR;
    int i;
    int createdims = 0; /* 1 => we need to create the dims in root if they do not already exist */
    int purezarr = 0;
    int xarray = 0;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
    NCjson* jatts = NULL;

    ZTRACE(3,"file=%s var=%s purezarr=%d xarray=%d ndims=%d shape=%s",
        file->controller->path,var->hdr.name,purezarr,xarray,(int)ndims,nczprint_vector(ndims,shapes));

    if(zfile->flags & FLAG_PUREZARR) purezarr = 1;
    if(zfile->flags & FLAG_XARRAYDIMS) xarray = 1;

    if(purezarr && xarray) {/* Read in the attributes to get xarray dimdef attribute; Note that it might not exist */
        /* Note that if xarray && !purezarr, then xarray will be superceded by the nczarr dimensions key */
        char zdimname[4096];
        if(zvar->xarray == NULL) {
            assert(nclistlength(dimnames) == 0);
            if((stat = NCZ_read_attrs(file,(NC_OBJ*)var,NULL))) goto done;
        }
        if(zvar->xarray != NULL) {
            /* convert xarray to the dimnames */
            for(i=0;i<nclistlength(zvar->xarray);i++) {
                snprintf(zdimname,sizeof(zdimname),"/%s",(const char*)nclistget(zvar->xarray,i));
                nclistpush(dimnames,strdup(zdimname));
            }
        }
        createdims = 1; /* may need to create them */
    }

    /* If pure zarr and we have no dimref names, then fake it */
    if(purezarr && nclistlength(dimnames) == 0) {
        createdims = 1;
        for(i=0;i<ndims;i++) {
            /* Compute the set of absolute paths to dimrefs */
            char zdimname[4096];
            snprintf(zdimname,sizeof(zdimname),"/%s_%llu",ZDIMANON,shapes[i]);
            nclistpush(dimnames,strdup(zdimname));
        }
    }

    /* Now, use dimnames to get the dims; create if necessary */
    if((stat = NCZ_parsedimrefs(file,dimnames,shapes,dims,createdims)))
        goto done;

done:
    NCJreclaim(jatts);
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create)
{
    int i, stat = NC_NOERR;
    NClist* segments = NULL;

    for(i=0;i<nclistlength(dimnames);i++) {
        NC_GRP_INFO_T* g = NULL;
        NC_DIM_INFO_T* d = NULL;
        int j;
        const char* dimpath = nclistget(dimnames,i);
        const char* dimname = NULL;

        /* Locate the corresponding NC_DIM_INFO_T* object */
        nclistfreeall(segments);
        segments = nclistnew();
        if((stat = ncz_splitkey(dimpath,segments)))
            goto done;
        if((stat=NCZ_locategroup(file,nclistlength(segments)-1,segments,&g)))
            goto done;
        /* Lookup the dimension */
        dimname = nclistget(segments,nclistlength(segments)-1);
        d = NULL;
        dims[i] = NULL;
        for(j=0;j<ncindexsize(g->dim);j++) {
            d = (NC_DIM_INFO_T*)ncindexith(g->dim,j);
            if(strcmp(d->hdr.name,dimname)==0) {
                dims[i] = d;
                break;
            }
        }
        if(dims[i] == NULL && create) {
            /* If not found and create then create it */
            if((stat = NCZ_createdim(file, dimname, shape[i], &dims[i])))
                goto done;
        } else {
            /* Verify consistency */
            if(dims[i]->len != shape[i])
                {stat = NC_EDIMSIZE; goto done;}
        }
        assert(dims[i] != NULL);
    }
done:
    nclistfreeall(segments);
    return THROW(stat);
}

/* Convert an attribute types list to an nc_type list */
static int
NCZ_jtypes2atypes(int purezarr, const NCjson* jattrs, const NCjson* jtypes, nc_type** atypesp)
{
    int stat = NC_NOERR;
    size_t i;
    nc_type* atypes = NULL;
    
    if(jtypes != NULL && NCJdictlength(jtypes) != NCJdictlength(jattrs)) {stat = NC_ENCZARR; goto done;} /* length mismatch */
    if((atypes = (nc_type*)calloc(NCJdictlength(jattrs),sizeof(nc_type)))==NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<NCJdictlength(jattrs);i++) {
        const NCjson* akey = NCJdictkey(jattrs,i);
        if(NCJsort(akey) != NCJ_STRING) {stat = NC_ENOTZARR; goto done;}
        if(jtypes == NULL) {
            const NCjson* avalues = NCJdictvalue(jattrs,i);
            /* Infer the type from the values */
            if((stat = NCZ_inferattrtype(avalues,NC_NAT,&atypes[i]))) goto done;
        } else {
	    /* Find corresponding entry in the types dict */
            const NCjson* jtype = NULL;
            /* Get the nc_type */
	    NCJdictget(jtypes,NCJstring(akey),&jtype);
            if((stat = ncz2_dtype2nctype(NCJstring(jtype),NC_NAT,purezarr,&atypes[i],NULL,NULL))) goto done;
        }
    }
    if(atypesp) {*atypesp = atypes; atypes = NULL;}
done:
    nullfree(atypes);
    return stat;
}

/*
Given a list of segments, find corresponding group.
*/
static int
NCZ_locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp)
{
    int i, j, found, stat = NC_NOERR;
    NC_GRP_INFO_T* grp = NULL;

    grp = file->root_grp;
    for(i=0;i<nsegs;i++) {
        const char* segment = nclistget(segments,i);
        char norm_name[NC_MAX_NAME];
        found = 0;
        if((stat = nc4_check_name(segment,norm_name))) goto done;
        for(j=0;j<ncindexsize(grp->children);j++) {
            NC_GRP_INFO_T* subgrp = (NC_GRP_INFO_T*)ncindexith(grp->children,j);
            if(strcmp(subgrp->hdr.name,norm_name)==0) {
                grp = subgrp;
                found = 1;
                break;
            }
        }
        if(!found) {stat = NC_ENOGRP; goto done;}
    }
    /* grp should be group of interest */
    if(grpp) *grpp = grp;

done:
    return THROW(stat);
}

/* This code is a subset of NCZ_def_dim */
static int
NCZ_createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp)
{
    int stat = NC_NOERR;
    NC_GRP_INFO_T* root = file->root_grp;
    NC_DIM_INFO_T* thed = NULL;
    if((stat = nc4_dim_list_add(root, name, (size_t)dimlen, -1, &thed)))
        goto done;
    assert(thed != NULL);
    /* Create struct for NCZ-specific dim info. */
    if (!(thed->format_dim_info = calloc(1, sizeof(NCZ_DIM_INFO_T))))
        {stat = NC_ENOMEM; goto done;}
    ((NCZ_DIM_INFO_T*)thed->format_dim_info)->common.file = file;
    *dimp = thed; thed = NULL;
done:
    return stat;
}

static int
ZF2_buildchunkkey(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    int r;
    NCbytes* key = ncbytesnew();

    if(keyp) *keyp = NULL;

    assert(islegaldimsep(dimsep));

    for(r=0;r<rank;r++) {
        char sindex[64];
        if(r > 0) ncbytesappend(key,dimsep);
        /* Print as decimal with no leading zeros */
        snprintf(sindex,sizeof(sindex),"%lu",(unsigned long)chunkindices[r]);
        ncbytescat(key,sindex);
    }
    ncbytesnull(key);
    if(keyp) *keyp = ncbytesextract(key);

    ncbytesfree(key);
    return THROW(stat);
}

/**************************************************/
/* Format Filter Support Functions */

/* JSON Parse/unparse of filter codecs */

#ifdef ENABLE_NCZARR_FILTERS
int
ZF2_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;

    /* Convert the HDF5 id + visible parameters to the codec form */

    /* Clear any previous codec */
    nullfree(filter->codec.id); filter->codec.id = NULL;
    nullfree(filter->codec.codec); filter->codec.codec = NULL;
    filter->codec.id = strdup(filter->plugin->codec.codec->codecid);
    if(filter->plugin->codec.codec->NCZ_hdf5_to_codec) {
        stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(NCplistzarrv2,filter->hdf5.id,filter->hdf5.visible.nparams,filter->hdf5.visible.params,&filter->codec.codec);
#ifdef DEBUGF
        fprintf(stderr,">>> DEBUGF: NCZ_hdf5_to_codec: visible=%s codec=%s\n",printnczparams(filter->hdf5.visible),filter->codec.codec);
#endif
        if(stat) goto done;
    } else
        {stat = NC_EFILTER; goto done;}

done:
    return THROW(stat);
}

/* Build filter from parsed Zarr metadata */
int
ZF2_codec2hdf(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, NCZ_Plugin* plugin)
{
    int stat = NC_NOERR;
    const NCjson* jvalue = NULL;

    assert(jfilter != NULL);
    assert(filter != NULL);

    if(filter->codec.id == NULL) {
        /* Get the id of this codec filter */
        if(NCJdictget(jfilter,"id",&jvalue)<0) {stat = NC_EFILTER; goto done;}
        if(!NCJisatomic(jvalue)) {stat = THROW(NC_ENOFILTER); goto done;}
        filter->codec.id = strdup(NCJstring(jvalue));
    }

    if(filter->codec.codec == NULL) {
        /* Unparse jfilter */
        if(NCJunparse(jfilter,0,&filter->codec.codec)<0) goto done;
    }

    if(plugin != NULL) {
        /* Save the hdf5 id */
        filter->hdf5.id = plugin->hdf5.filter->id;
        /* Convert the codec to hdf5 form visible parameters */
        if(plugin->codec.codec->NCZ_codec_to_hdf5) {
            stat = plugin->codec.codec->NCZ_codec_to_hdf5(NCplistzarrv2,filter->codec.codec,&filter->hdf5.id,&filter->hdf5.visible.nparams,&filter->hdf5.visible.params);
#ifdef DEBUGF
            fprintf(stderr,">>> DEBUGF: NCZ_codec_to_hdf5: codec=%s, hdf5=%s\n",printcodec(codec),printhdf5(hdf5));
#endif
            if(stat) goto done;
        }
    }

done:
    return THROW(stat);
}
#endif /*ENABLE_NCZARR_FILTERS*/

/**************************************************/
/* Format Dispatch table */

static const NCZ_Formatter NCZ_formatter2_table = {
    NCZARRFORMAT2,
    ZARRFORMAT2,
    NCZ_FORMATTER_VERSION,

    ZF2_create,
    ZF2_open,
    ZF2_close,
    ZF2_readmeta,
    ZF2_writemeta,
    ZF2_readattrs,
    ZF2_buildchunkkey,
#ifdef ENABLE_NCZARR_FILTERS
    ZF2_codec2hdf,
    ZF2_hdf2codec,
#else
    NULL,
    NULL,
#endif
};

const NCZ_Formatter* NCZ_formatter2 = &NCZ_formatter2_table;

int
NCZF2_initialize(void)
{
    return NC_NOERR;
}

int
NCZF2_finalize(void)
{
    return NC_NOERR;
}
