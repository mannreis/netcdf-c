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
/* Big endian Bytes filter */
static const char* NCZ_Bytes_Big_Text = "{\"name\": \"bytes\", \"configuration\": {\"endian\": \"big\"}}";
NCjson* NCZ_Bytes_Big_Json = NULL;

/* Little endian Bytes filter */
static const char* NCZ_Bytes_Little_Text = "{\"name\": \"bytes\", \"configuration\": {\"endian\": \"little\"}}";
NCjson* NCZ_Bytes_Little_Json = NULL;

/**************************************************/

/*Forward*/
static int ZF3_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF3_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF3_close(NC_FILE_INFO_T* file);
static int ZF3_writemeta(NC_FILE_INFO_T* file);
static int ZF3_readmeta(NC_FILE_INFO_T* file);
static int ZF3_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container);
static int ZF3_buildchunkkey(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);
#ifdef ENABLE_NCZARR_FILTERS
static int ZF3_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF3_codec2hdf(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, NCZ_Plugin* plugin);
#endif

static int write_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp);
static int write_var_meta(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);
static int write_var(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);

static int build_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NC_OBJ* container, NCindex* attlist, NCjson** jattsp, NCjson** jnczattsp);
static int build_superblock(NC_FILE_INFO_T* file, NCjson** jsuperp);
static int build_group_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** grpp);

static int read_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
static int read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
static int read_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);
static int NCZ_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jblock);
static int parse_superblock(NC_FILE_INFO_T* file, NCjson* jsuper);
static int parse_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);

static int NCZ_collect_grps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NCjson** jgrpsp);
static int NCZ_collect_arrays(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NCjson** jarraysp);
static int NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NCjson** jdimsp);

static int NCZ_decodeints(NCjson* jshape, size64_t* shapes);
static int NCZ_computeattrinfo(const char* name, NClist* atypes, nc_type typehint, int purezarr, NCjson* values, nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap);
static int NCZ_computeattrdata(nc_type typehint, nc_type* typeidp, NCjson* values, size_t* typelenp, size_t* countp, void** datap);
static int NCZ_computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jvar, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);
static int NCZ_load_jatts(NCjson* block, NCjson** jattrsp, NClist** atypesp);
static int NCZ_json_convention_read(NCjson* json, NCjson** jtextp);
static int NCZ_attr_convert(NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst);
static int NCZ_jtypes2atypes(NCjson* jtypes, NClist* atypes);
static int NCZ_charify(NCjson* src, NCbytes* buf);
static int subobjects_pure(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* grpnames);
static int subobjects(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* grpnames);
static int finddim(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* dimname, size64_t size, NC_DIM_INFO_T** dimp);

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
ZF3_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

static int
ZF3_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
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
 *         ::NC_EXXX errors
 * @author Dennis Heimbigner
 */
static int
ZF3_writemeta(NC_FILE_INFO_T* file)
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
    int purezarr = 0;
    int rootgrp = 0;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* jgroup = NULL;
    NCjson* jsuper = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;

    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;
    rootgrp = (grp->parent == NULL);

    /* Construct grp key */
    if((stat = NCZ_grpkey(grp,&fullpath)))
	goto done;

    /* If the group zarr.info has attributes, or group is root and not purezarr
       then build Z3GROUP contents
    */
    if((!purezarr && rootgrp) || ncindexsize(grp->att) > 0) {
        if((stat = NCJnew(NCJ_DICT,&jgroup))) goto done;
        if((stat = NCJinsertstring(jgroup,"node_type","group"))) goto done;
        if((stat = NCJinsertint(jgroup,"zarr_format",zfile->zarr.zarr_format))) goto done;
        /* Insert the group attributes */
        /* Build the attributes dictionary */
        assert(grp->att);
        if((stat = build_atts(file,zfile,(NC_OBJ*)grp, grp->att, &jatts, &jtypes))) goto done;
        if((stat = NCJinsert(jgroup,"attributes",jatts))) goto done;
        if(!purezarr && jtypes)
            {if((stat = NCJinsert(jgroup,NCZ_V3_ATTR,jtypes))) goto done;}
    }

    if(!purezarr && rootgrp) {
	assert(jgroup != NULL);
        /* Build the superblock */
	if((stat = build_superblock(file,&jsuper))) goto done;
    }

    /* Insert superblock into root group */
    if(jsuper != NULL) {
	assert(jgroup != NULL);
	/* Disable must_understand */
	if((stat = NCJinsertstring(jgroup,"must_understand","false"))) goto done;
	assert(jgroup != NULL);
	if((stat = NCJinsert(jgroup,NCZ_V3_SUPERBLOCK,jsuper))) goto done;
	jsuper = NULL;
    }

    if(jgroup != NULL) {
        /* build Z3GROUP path */
        if((stat = nczm_concat(fullpath,Z3GROUP,&key))) goto done;
        /* Write to map */
        if((stat=NCZ_uploadjson(map,key,jgroup))) goto done;
        nullfree(key); key = NULL;
        jgroup = NULL;
    }

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
    nullfree(key);
    nullfree(fullpath);
    NCJreclaim(jgroup);
    NCJreclaim(jsuper);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
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
    char tmpstr[1024];
    char* fullpath = NULL;
    char* key = NULL;
    char* dimpath = NULL;
    NClist* dimrefs = NULL;
    NCjson* jvar = NULL;
    NCjson* jncvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jdimnames = NULL;
    NCjson* jtmp = NULL;
    NCjson* jtmp2 = NULL;
    NCjson* jtmp3 = NULL;
    NCjson* jfill = NULL;
    NCjson* jcodecs = NULL;
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

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;

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

    /* Create the variable's metadata json dict */
    if((stat = NCJnew(NCJ_DICT,&jvar)))
	goto done;

    /* build Z3ARRAY contents */
    if((stat = NCJinsertstring(jvar,"node_type","array"))) goto done;

    if(!purezarr) {
	/* Disable must_understand */
	if((stat = NCJinsertstring(jvar,"must_understand","false"))) goto done;
    }

    /* zarr_format key */
    if((stat = NCJinsertint(jvar,"zarr_format",zfile->zarr.zarr_format))) goto done;

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
    if((stat = NCJnew(NCJ_ARRAY,&jtmp))) goto done;
    if(!zvar->scalar) {
        for(i=0;i<var->ndims;i++) {
	    snprintf(tmpstr,sizeof(tmpstr),"%llu",shape[i]);
	    NCJaddstring(jtmp,NCJ_INT,tmpstr);
	}
    }
    if((stat = NCJinsert(jvar,"shape",jtmp))) goto done;
    jtmp = NULL;

    /* data_type key */
    /* A string or list defining a valid data type for the array. */
    {	/* compute the type name */
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = ncz3_nctype2dtype(atomictype,purezarr,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename))) goto done;
	if((stat = NCJinsertstring(jvar,"data_type",dtypename))) goto done;
	nullfree(dtypename); dtypename = NULL;
    }

    /* chunk_grid key {"name": "regular", "configuration": {"chunk_shape": [n1, n2, ...]}}  */
        /* The zarr format does not support the concept
           of contiguous (or compact), so it will never appear in the read case.
        */
    /* create the chunk sizes list */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp3))) goto done;
    if(zvar->scalar) {
	NCJaddstring(jtmp3,NCJ_INT,"1"); /* one chunk of size 1 */
    } else for(i=0;i<var->ndims;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(tmpstr,sizeof(tmpstr),"%lld",len);
	NCJaddstring(jtmp3,NCJ_INT,tmpstr);
    }
    /* chunk_shape configuration dict */
    if((stat = NCJnew(NCJ_DICT,&jtmp2))) goto done;
    if((stat = NCJinsert(jtmp2,"chunk_shape",jtmp3))) goto done;
    jtmp3 = NULL;

    /* Assemble chunk_grid */
    if((stat = NCJnew(NCJ_DICT,&jtmp))) goto done;
    if((stat = NCJinsertstring(jtmp,"name","regular"))) goto done;
    if((stat = NCJinsert(jtmp,"configuration",jtmp2))) goto done;
    jtmp2 = NULL;
    if((stat = NCJinsert(jvar,"chunk_grid",jtmp))) goto done;    
    jtmp = NULL;    

    /* chunk_key_encoding key */

    /* chunk_key_encoding configuration key */
    if((stat = NCJnew(NCJ_DICT,&jtmp2))) goto done;
    tmpstr[0] = zvar->dimension_separator;
    tmpstr[1] = '\0';
    if((stat = NCJinsertstring(jtmp2,"separator",tmpstr))) goto done;

    /* assemble chunk_key_encoding dict */
    if((stat = NCJnew(NCJ_DICT,&jtmp))) goto done;
    if((stat = NCJinsertstring(jtmp,"name","default"))) goto done;
    if((stat = NCJinsert(jtmp,"configuration",jtmp2))) goto done;
    jtmp2 = NULL;
    if((stat = NCJinsert(jvar,"chunk_key_encoding",jtmp))) goto done;        
    jtmp = NULL;

    /* fill_value key */
    if(var->no_fill) {
	if((stat=NCJnew(NCJ_NULL,&jfill))) goto done;
    } else {/*!var->no_fill*/
	int atomictype = var->type_info->hdr.id;
        if(var->fill_value == NULL) {
	     if((stat = NCZ_ensure_fill_value(var))) goto done;
	}
        /* Convert var->fill_value to a string */
	if((stat = NCZ_stringconvert(atomictype,1,var->fill_value,&jfill))) goto done;
	assert(jfill->sort != NCJ_ARRAY);
    }
    if((stat = NCJinsert(jvar,"fill_value",jfill))) goto done;
    jfill = NULL;

    /* codecs key */
    /* A list of JSON objects providing codec configurations; note that this is never empty
       because endianness must always be included. */
    /* Add the endianness codec as first entry */
    
#ifdef ENABLE_NCZARR_FILTERS
    /* jcodecs holds the array of filters */
    if((stat = NCJnew(NCJ_ARRAY,&jcodecs))) goto done;
    /* Insert the "bytes" codec as first (pseudo-)codec */
    {
	NCjson* bytescodec = NULL;
	int endianness = var->endianness;
	if(endianness == NC_ENDIAN_NATIVE)
	    endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
	if(endianness == NC_ENDIAN_LITTLE) bytescodec = NCZ_Bytes_Little_Json;
	else {assert(endianness == NC_ENDIAN_LITTLE); bytescodec = NCZ_Bytes_Little_Json;}
	if(NCJclone(bytescodec,&jtmp) || jtmp == NULL) {stat = NC_ENOTZARR; goto done;}
        if(NCJappend(jcodecs,jtmp)) goto done;
	jtmp = NULL;
    }

    /* Get chain of filters for this variable */
    filterchain = (NClist*)var->filters;
    if(nclistlength(filterchain) > 0) {
	int k;
	for(k=0;k<nclistlength(filterchain);k++) {
 	    struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,k);
	    /* encode up the filter as a string */
	    if((stat = NCZ_filter_jsonize(file,var,filter,&jfilter))) goto done;
	    if((stat = NCJappend(jcodecs,jfilter))) goto done;
	    jfilter = NULL;
	}
    }
#endif
    if((stat = NCJinsert(jvar,"codecs",jcodecs))) goto done;
    jcodecs = NULL;

    /* build "dimension_names" key */
    if(NCJnew(NCJ_ARRAY,&jdimnames)) {stat = NC_ENOTZARR; goto done;}
     for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	if(NCJaddstring(jdimnames,NCJ_STRING, dim->hdr.name)) {stat = NC_ENOTZARR; goto done;}
    }
    if(NCJinsert(jvar,"dimension_names",jdimnames)) {stat = NC_ENOTZARR; goto done;}
    jdimnames = NULL;

    /* Capture dimref names as FQNs; simultaneously collect the simple dim names */
    if(!purezarr && var->ndims > 0) {
        if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_dimkey(dim,&dimpath))) goto done;
	    nclistpush(dimrefs,dimpath);
	    dimpath = NULL;
	}
    }

    /* Build the NCZ_V3_ARRAY dict entry */
    if(!purezarr) {
	/* Create the dimrefs json object */
	if((stat = NCJnew(NCJ_ARRAY,&jdimrefs)))
	    goto done;
	for(i=0;i<nclistlength(dimrefs);i++) {
	    const char* dim = nclistget(dimrefs,i);
	    NCJaddstring(jdimrefs,NCJ_STRING,dim);
	}

        /* create the "_nczarr_array" dict */
	if((stat = NCJnew(NCJ_DICT,&jncvar))) goto done;	

	/* Insert dimrefs  */
	if((stat = NCJinsert(jncvar,"dimensions",jdimrefs)))
	    goto done;
	jdimrefs = NULL; /* Avoid memory problems */

	if(!purezarr) {
	    if((stat = NCJinsert(jvar,NCZ_V3_ARRAY,jncvar))) goto done;
	    jncvar = NULL;
	}
    }

    /* build zarr.json path */
    if((stat = nczm_concat(fullpath,Z3ARRAY,&key)))
	goto done;

    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jvar)))
	goto done;
    nullfree(key); key = NULL;
    NCJreclaim(jvar); jvar = NULL;

    var->created = 1;

done:
    nclistfreeall(dimrefs);
    nullfree(fullpath);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jncvar);
    NCJreclaim(jtmp);
    NCJreclaim(jtmp2);
    NCJreclaim(jfill);
    NCJreclaim(jcodecs);
    NCJreclaim(jdimnames);
    NCJreclaim(jdimrefs);
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
 * @internal Convert an objects attributes to a JSON dictionary.
 *
 * @param file
 * @param zfile
 * @param container
 * @param attlist
 * @param jattsp return dictionary in this
 * @param jnczattsp return _nczarr_attrs dictionary
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
build_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NC_OBJ* container, NCindex* attlist, NCjson** jattsp, NCjson** jnczattsp)
{
    int i,stat = NC_NOERR;
    NCjson* jatts = NULL;
    NCjson* jnczatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jtype = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    int purezarr = 0;
    NC_VAR_INFO_T* var = NULL;
    char* tname = NULL;

    ZTRACE(3,"file=%s container=%s |attlist|=%u",file->controller->path,container->name,(unsigned)ncindexsize(attlist));
    
    if(container->sort == NCVAR)
        var = (NC_VAR_INFO_T*)container;

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;

    /* Create the attribute dictionary */
    if((stat = NCJnew(NCJ_DICT,&jatts))) goto done;

    /* Create the attribute types dict */
    if(!purezarr)
        {if((stat = NCJnew(NCJ_DICT,&jtypes))) goto done;}

    if(ncindexsize(attlist) > 0) {
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(attlist);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	    size_t typesize = 0;
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
            /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;
	    if((stat = NCJinsert(jatts,a->hdr.name,jdata))) goto done;
	    jdata = NULL;

	    if(!purezarr) {
	        /* Collect the corresponding dtype */
	        if((stat = ncz3_nctype2dtype(a->nc_typeid,purezarr,typesize,&tname))) goto done;
  	        if((stat = NCJnewstring(NCJ_STRING,tname,&jtype))) goto done;
	        nullfree(tname); tname = NULL;
	        if((stat = NCJinsert(jtypes,a->hdr.name,jtype))) goto done; /* add {name: type} */
	        jtype = NULL;
	    }
        }
    }
    /* Add other special attributes: Quantize */
    if(container->sort == NCVAR && var && var->quantize_mode > 0) {    
	char mode[64];
	snprintf(mode,sizeof(mode),"%d",var->nsd);
        if((stat = NCJnewstring(NCJ_INT,mode,&jint)))
	        goto done;
	/* Insert the quantize attribute */
	switch (var->quantize_mode) {
	case NC_QUANTIZE_BITGROOM:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITGROOM_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_GRANULARBR:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_GRANULARBR_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_BITROUND:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITROUND_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	default: break;
	}
    }

    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jnczattsp) {*jnczattsp = jnczatts; jnczatts = NULL;}
done:
    NCJreclaim(jtypes);
    NCJreclaim(jtype);
    NCJreclaim(jdimrefs);
    NCJreclaim(jint);
    NCJreclaim(jdata);
    return THROW(stat);
}

/**
The super block contains a JSON tree representing part of the
so-called combined metadata information.
It contains the group names, the dimension objects,
and the array names.
Eventually,if the Zarr V3 spec defines the combined metadata,
then that will be used instead of this information.

The general form is this,
where the topmost dictionary node represents the root group:
_nczarr_superblock: {
    version: 3.0.0,    
    format: 3,
    root: {
        dimensions: {name: <dimname>, size: <integer>, unlimited: 1|0},
        arrays: [{name: <name>},...],
        children: [
                {name: <name>,
                dimensions: {name: <dimname>, size: <integer>, unlimited: 1|0},
                arrays: [{name: <name>},...],
                children: [{name: <name>, subgrps: [...]},{name: <name>, subgrps: [...]},...]
                },
                ...
        ],
    }
}

*/

static int
build_superblock(NC_FILE_INFO_T* file, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    NCjson* jroot = NULL;
    NCjson* jsuper = NULL;    
    char version[1024];
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;

    if((stat = NCJnew(NCJ_DICT,&jsuper))) goto done;

    /* Fill in superblock */

    /* Track the library version that wrote this */
    strncpy(version,NCZARR_PACKAGE_VERSION,sizeof(version));
    NCJinsertstring(jsuper,"version",version);
    NCJinsertint(jsuper,"format",zfile->zarr.nczarr_format);

    /* Capture and insert the subgroup skeleton */
    if((stat = build_group_json(file,file->root_grp,&jroot))) goto done;
    NCJinsert(jsuper, "root", jroot);
    jroot = NULL;

    if(jsuperp) {*jsuperp = jsuper; jsuper = NULL;}
done:
    NCJreclaim(jroot);
    NCJreclaim(jsuper);
    return THROW(stat);
}

static int
build_group_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** grpp)
{
    int stat = NC_NOERR;
    NCjson* jgrp = NULL;
    NCjson* jsubgrps = NULL;
    NCjson* jdims = NULL;
    NCjson* jarrays = NULL;
    NCjson* jname = NULL;

    /* Collect the dimensions in this group */
    if((stat = NCZ_collect_dims(file, grp, &jdims))) goto done;

    /* Collect the arrays in this group */
    if((stat = NCZ_collect_arrays(file, grp, &jarrays))) goto done;

    /* Collect the subgroups in this group */
    if((stat = NCZ_collect_grps(file, grp, &jsubgrps))) goto done;

    /* Fill in the grp dict */
    NCJnew(NCJ_DICT,&jgrp);
    NCJnewstring(NCJ_STRING,grp->hdr.name,&jname);
    NCJinsert(jgrp,"name",jname);
    jname = NULL;
    NCJinsert(jgrp,"dimensions",jdims);
    jdims = NULL;
    NCJinsert(jgrp,"arrays",jarrays);
    jarrays = NULL;
    NCJinsert(jgrp,"children",jsubgrps);
    jsubgrps = NULL;

    if(grpp) {*grpp = jgrp; jgrp = NULL;}

done:
    NCJreclaim(jgrp);
    NCJreclaim(jsubgrps);
    NCJreclaim(jdims);
    NCJreclaim(jarrays);
    NCJreclaim(jname);
    return THROW(stat);
}

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
ZF3_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCjson* jblock = NULL;
    NCjson* jsuper = NULL;

    ZTRACE(3,"file=%s",file->controller->path);
    
    zfile = file->format_file_info;

    /* Read the root group's metadata */
    switch(stat = NCZ_downloadjson(zfile->map, Z3METAROOT, &jblock)) {
    case NC_EEMPTY: /* not there */
	zfile->controls.flags |= FLAG_PUREZARR;
	stat = NC_NOERR; /* reset */
	goto done;
    case NC_NOERR:
	/* See if _nczarr_superblock key exists */
	NCJdictget(jblock,NCZ_V3_SUPERBLOCK,&jsuper);
	if(jsuper != NULL) {
	    /* in any case this is nczarr format 3 */
	    if((stat = parse_superblock(file,jsuper))) goto done;
	}
	break;
    default: goto done;
    }

    /* Now load the groups contents starting with root */
    if((stat = read_grp(file,file->root_grp)))
	goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
Used to support lazy attribute read.
@param file - [in] the containing file (annotation)
@param container - [in] the containing object (var or grp)
@return NC_NOERR|NC_EXXX

@author Dennis Heimbigner
*/
static int
ZF3_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container)
{
    int stat = NC_NOERR;

    /* Zarr V3 stores attributes in zinfo.json. This means that it is
       useless to support lazy attribute read.
    */
    return THROW(stat);
}

static int
ZF3_buildchunkkey(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    int r;
    NCbytes* key = ncbytesnew();

    if(keyp) *keyp = NULL;

    assert(islegaldimsep(dimsep));
    
    ncbytescat(key,"c");
    ncbytesappend(key,dimsep);
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

static int
ZF3_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/

static int
parse_superblock(NC_FILE_INFO_T* file, NCjson* jsuper)
{
    int stat = NC_NOERR;
    NCjson* jvalue = NULL;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_GRP_INFO_T* root = (NC_GRP_INFO_T*)file->root_grp;
    NCZ_GRP_INFO_T* zroot = (NCZ_GRP_INFO_T*)root->format_grp_info;

    NCJdictget(jsuper,"version",&jvalue);
    if(jvalue != NULL) {
	sscanf(NCJstring(jvalue),"%d.0.0",&zfile->zarr.nczarr_format);
	assert(zfile->zarr.nczarr_format == NCZARRFORMAT3);
    }
    NCJdictget(jsuper,"format",&jvalue);
    if(jvalue != NULL) {
	sscanf(NCJstring(jvalue),"%d",&zfile->zarr.zarr_format);
	assert(zfile->zarr.zarr_format == ZARRFORMAT3);
    }
    zfile->superblock = jsuper;
    NCJdictget(jsuper,"root",&jvalue);
    if(jvalue == NULL) {stat = NC_ENCZARR; goto done;}
    zroot->grpsuper = jvalue;

done:
    return THROW(stat);
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
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZMAP* map = zfile->map;
    char* fullpath = NULL;
    char* key = NULL;
    int purezarr = 0;
    NCjson* jgroup = NULL;
    NClist* subvars = nclistnew();
    NClist* subgrps = nclistnew();

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);
    
    purezarr = (zfile->controls.flags & FLAG_PUREZARR);

    /* Construct grp path */
    if((stat = NCZ_grpkey(grp,&fullpath)))
	goto done;

    /* Download the grp meta-data */
    /* build Z3GROUP path */
    if((stat = nczm_concat(fullpath,Z3GROUP,&key))) goto done;
    /* Read */
    jgroup = NULL;
    stat=NCZ_downloadjson(map,key,&jgroup);
    nullfree(key); key = NULL;
    if(!purezarr && !jgroup) {stat = NC_ENCZARR; goto done;}

    /* Define dimensions */
    if((stat = parse_dims(file,grp))) goto done;

    /* Define attributes */
    if((stat = NCZ_read_atts(file,(NC_OBJ*)grp,jgroup))) goto done;

    /* Pull out lists about groups and vars */
    if(purezarr)
        {if((stat = subobjects_pure(zfile,grp,subvars,subgrps))) goto done;}
    else
        {if((stat = subobjects(zfile,grp,subvars,subgrps))) goto done;}

    /* Define vars */
    if((stat = read_vars(file,grp,subvars))) goto done;

    /* Read sub-groups */
    if((stat = read_subgrps(file,grp,subgrps))) goto done;

done:
    nclistfreeall(subvars);
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
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
parse_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int i,stat = NC_NOERR;
    NCZ_GRP_INFO_T* zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;
    NCjson* jdims = NULL;
    NCjson* jdim = NULL;
    NCjson* jname = NULL;    
    NCjson* jsize = NULL;
    NCjson* junlim = NULL;
    NCjson* jcfg = NULL;
    
    ZTRACE(3,"file=%s grp=%s |diminfo|=%u",file->controller->path,grp->hdr.name,nclistlength(diminfo));

    /* Get dim defs for this group */
    NCJdictget(zgrp->grpsuper,"dimensions",&jdims);
    if(jdims == NULL) goto done; /* no dims to create */
    assert(NCJsort(jdims)==NCJ_ARRAY);
    /* Reify each dim in turn */
    for(i = 0; i < NCJlength(jdims); i++) {
        NC_DIM_INFO_T* dim = NULL;
        size64_t dimlen = 0;
        int isunlim = 0;
    
        /* Extract info */
        jdim = NCJith(jdims,i);
        assert(NCJsort(jdim) == NCJ_DICT);
        NCJdictget(jdim,"name",&jname);
        assert(jname != NULL);
        NCJdictget(jdim,"configuration",&jcfg);
        assert(jcfg != NULL);
        NCJdictget(jcfg,"size",&jsize);
	assert(jsize != NULL);
        NCJdictget(jcfg,"unlimited",&junlim); /* might be null */
        
        /* Create the NC_DIM_INFO_T object */
        {
	    const char* name = NCJstring(jname);
	    unsigned long data;
            sscanf(NCJstring(jsize),"%lu",&data); /* Get length */
            dimlen = (size64_t)data;
            if(junlim != NULL) {
                sscanf(NCJstring(junlim),"%ld",&data); /* Get unlimited flag */
                isunlim = (data == 0 ? (int)0 : (int)1);
            } else
                isunlim = 0;
            if((stat = nc4_dim_list_add(grp, name, (size_t)dimlen, -1, &dim))) goto done;
            dim->unlimited = isunlim;
            if((dim->format_dim_info = calloc(1,sizeof(NCZ_DIM_INFO_T))) == NULL)
                {stat = NC_ENOMEM; goto done;}
            ((NCZ_DIM_INFO_T*)dim->format_dim_info)->common.file = file;
        }
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize vars into memory;
 * Take purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varlist list of var names in this group
 * @param varnames List of names of variables in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int i,j,k,stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZMAP* map = zfile->map;
    int purezarr = 0;

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    if(zfile->controls.flags & FLAG_PUREZARR) purezarr = 1;

    if(nclistlength(varnames) == 0) goto done; /* Nothing to create */

    /* Reify each array in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	/* per-variable info */
        NC_VAR_INFO_T* var = NULL;
        NCZ_VAR_INFO_T* zvar = NULL;
        NCjson* jvar = NULL;
        NCjson* jncvar = NULL;
        NCjson* jdimrefs = NULL;
        NCjson* jvalue = NULL;
        NCjson* jtmp = NULL;
        NCjson* jtmp2 = NULL;
        NCjson* jsep = NULL;
        NCjson* jcodecs = NULL;
        NCjson* jcodec = NULL;
        char* varpath = NULL;
        char* key = NULL;
	const char* varname = NULL;
        size64_t* shapes = NULL;
        size64_t* chunks = NULL;
        NClist* dimnames = NULL;
        int suppress = 0; /* Abort processing of this variable */
        nc_type vtype = NC_NAT;
        int vtypelen = 0;
        int rank = 0;
#ifdef ENABLE_NCZARR_FILTERS
        int varsized = 0;
        int chainindex = 0;
#endif

	varname = (const char*)nclistget(varnames,i);
        dimnames = nclistnew();

	/* Create the var */
	if((stat = nc4_var_list_add2(grp, varname, &var))) goto done;

	/* And its annotation */
	if((zvar = calloc(1,sizeof(NCZ_VAR_INFO_T)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	var->format_var_info = zvar;
	zvar->common.file = file;

        /* pretend it was created */
	var->created = 1;

	/* Indicate we do not have quantizer yet */
	var->quantize_mode = -1;

	/* Construct var path */
	if((stat = NCZ_varkey(var,&varpath))) goto done;

	/* Construct the path to the zarray object */
	if((stat = nczm_concat(varpath,Z3ARRAY,&key))) goto done;

	/* Download the zarray object */
	if((stat=NCZ_readdict(map,key,&jvar))) goto done;
	nullfree(key); key = NULL;
	assert(NCJsort(jvar) == NCJ_DICT);

        /* Extract the metadata from jvar */

	/* Verify the format */
	{
	    int version;
	    if((stat = NCJdictget(jvar,"node_type",&jvalue))) goto done;	    
	    if(strcasecmp("array",NCJstring(jvalue))!=0) {stat = THROW(NC_ENOTZARR); goto done;}
	    if((stat = NCJdictget(jvar,"zarr_format",&jvalue))) goto done;
	    sscanf(NCJstring(jvalue),"%d",&version);
	    if(version != zfile->zarr.zarr_format)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	}

	/* Get the type of the variable */
	{
	    if((stat = NCJdictget(jvar,"data_type",&jvalue))) goto done;
	    /* Convert dtype to nc_type */
	    if((stat = ncz3_dtype2nctype(NCJstring(jvalue),purezarr,&vtype,&vtypelen))) goto done;
	    if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
		/* Locate the NC_TYPE_INFO_T object */
		if((stat = ncz_gettype(file,grp,vtype,&var->type_info))) goto done;
	    } else {stat = NC_EBADTYPE; goto done;}
	    if(vtype == NC_STRING) {
		zvar->maxstrlen = vtypelen;
		vtypelen = sizeof(char*); /* in-memory len */
		if(zvar->maxstrlen <= 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
	    }
	}

	/* shape */
	{
	    if((stat = NCJdictget(jvar,"shape",&jvalue))) goto done;
	    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = (THROW(NC_ENCZARR)); goto done;}
	    
	    if(NCJlength(jvalue) == 0) {
	        zvar->scalar = 1;
		rank = 0;		
	    } else {
		zvar->scalar = 0;
	        rank = NCJlength(jvalue);
	    }
	    
	    if(rank > 0) {
  	        /* Save the rank of the variable */
	        if((stat = nc4_var_set_ndims(var, rank))) goto done;
	        /* extract the shapes */
	        if((shapes = (size64_t*)malloc(sizeof(size64_t)*rank)) == NULL)
	            {stat = (THROW(NC_ENOMEM)); goto done;}
	        if((stat = NCZ_decodeints(jvalue, shapes))) goto done;
	    }
	    /* Set storage flag */
    	    var->storage = (zvar->scalar?NC_CONTIGUOUS:NC_CHUNKED);
	}

	if(!purezarr) {
  	    /* Extract the _NCZARR_ARRAY values */
	    if((stat = NCJdictget(jvar,NCZ_V3_ARRAY,&jncvar))) goto done;
	    if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
   	    assert((NCJsort(jncvar) == NCJ_DICT));
	    /* Extract dimrefs list  */
	    switch ((stat = NCJdictget(jncvar,"dimensions",&jdimrefs))) {
	    case NC_NOERR: /* Extract the dimref names */
		assert((NCJsort(jdimrefs) == NCJ_ARRAY));
		if(zvar->scalar) {
	   	    assert(NCJlength(jdimrefs) == 0);		   
		} else {
		    rank = NCJlength(jdimrefs);
		    for(j=0;j<rank;j++) {
		        const NCjson* dimpath = NCJith(jdimrefs,j);
		        assert(NCJisatomic(dimpath));
		        nclistpush(dimnames,strdup(NCJstring(dimpath)));
		    }
		}
		break;
	    case NC_EEMPTY: /* will simulate it from the shape of the variable */
		stat = NC_NOERR;
		break;
	    default: goto done;
	    }
	    jdimrefs = NULL;
	}

        /* Capture attributes in case following code needs it TODO: fix when/if we do lazy attribute read */
        if((stat = NCZ_read_atts(file,(NC_OBJ*)var,jvar))) goto done;

	/* Capture dimension_separator (must precede chunk cache creation) */
	{
	    NCglobalstate* ngs = NC_getglobalstate();
	    assert(ngs != NULL);
	    zvar->dimension_separator = 0;
	    if((stat = NCJdictget(jvar,"chunk_key_encoding",&jvalue))) goto done;
	    if(jvalue == NULL) {
	        /* If value is invalid, then use global default */
	        if(!islegaldimsep(zvar->dimension_separator))
	        zvar->dimension_separator = ngs->zarr.dimension_separator; /* use global value */
	        /* Verify its value */
	    } else {
		if(NCJsort(jvalue) != NCJ_DICT) {stat = NC_ENOTZARR; goto done;}
		if((stat = NCJdictget(jvalue,"name",&jtmp))) {stat = NC_ENOTZARR; goto done;}
		if(strcasecmp("default",NCJstring(jtmp))==0) {
		    if((stat = NCJdictget(jvalue,"configuration",&jtmp))) {stat = NC_ENOTZARR; goto done;}
		    if(jtmp != NULL) {
			if((stat = NCJdictget(jtmp,"separator",&jsep))) {stat = NC_ENOTZARR; goto done;}		    
		        if(jsep != NULL) {
		            if(NCJisatomic(jsep) && NCJstring(jsep) != NULL && strlen(NCJstring(jsep)) == 1)
		                zvar->dimension_separator = NCJstring(jsep)[0];
			} else
		            zvar->dimension_separator = '/';
		    } else
		        zvar->dimension_separator = '/';
		} else if(strcasecmp("v2",NCJstring(jtmp))==0) {
		    if((stat = NCJdictget(jvalue,"separator",&jsep))) {stat = NC_ENOTZARR; goto done;}		    
		    if(jsep != NULL) {
		        if(NCJsort(jsep) == NCJ_STRING && NCJstring(jsep) != NULL && strlen(NCJstring(jsep)) == 1)
		            zvar->dimension_separator = NCJstring(jsep)[0];
			else {stat = NC_ENOTZARR; goto done;}
		    } else
		        zvar->dimension_separator = '.';
		} else {stat = NC_ENOTZARR; goto done;}

	    }
	    assert(islegaldimsep(zvar->dimension_separator)); /* we are hosed */
	}

	/* fill_value; must precede calls to adjust cache */
	{
	    if((stat = NCJdictget(jvar,"fill_value",&jvalue))) goto done;
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

	/* chunks */
	{
            if(!zvar->scalar) {
                if((stat = NCJdictget(jvar,"chunk_grid",&jvalue))) goto done;
                if(jvalue == NULL) {stat = (THROW(NC_ENOTBUILT)); goto done;}
                if((stat = NCJdictget(jvalue,"name",&jtmp))) goto done;
                if(jtmp == NULL) {stat = (THROW(NC_ENOTZARR)); goto done;}
                if(strcasecmp("regular",NCJstring(jtmp))!=0) {stat = THROW(NC_ENOTZARR); goto done;}
                if((stat = NCJdictget(jvalue,"configuration",&jtmp))) goto done;
                if(jtmp == NULL) {stat = (THROW(NC_ENOTZARR)); goto done;}
                if((stat = NCJdictget(jtmp,"chunk_shape",&jtmp2))) goto done;
                if(jtmp2 == NULL) {stat = (THROW(NC_ENOTZARR)); goto done;}
                if(NCJsort(jtmp2) != NCJ_ARRAY) {stat = (THROW(NC_ENOTZARR)); goto done;}
    
                /* Verify the rank */
                assert(rank != 0);
                if(rank != NCJlength(jtmp2)) {stat = (THROW(NC_ENCZARR)); goto done;}
    
                /* Get the chunks and chunkproduct */
                if((chunks = (size64_t*)malloc(sizeof(size64_t)*rank)) == NULL)
                        {stat = (THROW(NC_ENOMEM)); goto done;}
                if((stat = NCZ_decodeints(jtmp2, chunks))) goto done;
                zvar->chunkproduct = 1;
                for(k=0;k<rank;k++) zvar->chunkproduct *= chunks[i];
                zvar->chunksize = zvar->chunkproduct * var->type_info->size;
                /* Create the cache */
                if((stat = NCZ_create_chunk_cache(var,zvar->chunksize,zvar->dimension_separator,&zvar->cache))) goto done;
            } else {/* zvar->scalar */
                zvar->chunkproduct = 1;
                zvar->chunksize = zvar->chunkproduct * var->type_info->size;
                var->chunksizes = NULL;
                zvar->chunksize = zvar->chunkproduct * var->type_info->size;
                /* Create the cache */
                if((stat = NCZ_create_chunk_cache(var,zvar->chunksize,zvar->dimension_separator,&zvar->cache))) goto done;
            }
            if((stat = NCZ_adjust_var_cache(var))) goto done;
	}
	
        /* codecs key */
        /* From V2 Spec: A list of JSON objects providing codec configurations,
           or null if no filters are to be applied. Each codec configuration
           object MUST contain a "id" key identifying the codec to be used. */
	{
	    NCJdictget(jvar,"codecs",&jcodecs);
	    assert(jcodecs != NULL);
	    /* Process the first codec which must be bytes, to get the endianness */
	    jcodec = NCJith(jcodecs,0);
	    if(NCJsort(jcodec) != NCJ_DICT) {stat = NC_ENOTZARR; goto done;}            
	    if((stat = NCJdictget(jcodec,"name",&jvalue))) goto done;
	    if(jvalue == NULL || strcmp(ZBYTES3,NCJstring(jvalue))!=0) {stat = NC_ENCZARR; goto done;}            
	    if((stat = NCJdictget(jcodec,"configuration",&jvalue))) goto done;
	    if(jvalue == NULL || NCJsort(jvalue) != NCJ_DICT) {stat = NC_ENCZARR; goto done;}            
	    if((stat = NCJdictget(jvalue,"endian",&jtmp))) goto done;
	    if(strcasecmp("big",NCJstring(jtmp))==0)
	        var->endianness = NC_ENDIAN_BIG;
	    else if(strcasecmp("little",NCJstring(jtmp))==0)
	        var->endianness = NC_ENDIAN_LITTLE;
	    else {stat = NC_EINVAL; goto done;}

#ifdef ENABLE_NCZARR_FILTERS
	    if(var->filters == NULL) var->filters = (void*)nclistnew();
   	    if(zvar->incompletefilters == NULL) zvar->incompletefilters = (void*)nclistnew();
	    chainindex = 0; /* track location of filter in the chain */
	    if((stat = NCZ_filter_initialize())) goto done;
	    if((stat = NCJdictget(jvar,"codecs",&jcodecs))) goto done;
	    if(jcodecs == NULL || NCJsort(jcodecs) == NCJ_NULL)
	        {stat = NC_ENOTZARR; goto done;}
	    if(NCJsort(jcodecs) != NCJ_ARRAY) {stat = NC_ENOTZARR; goto done;}
    	    if(NCJlength(jcodecs) == 0) {stat = NC_ENOTZARR; goto done;}

	    /* Process remaining filters */
	    for(k=1;;k++) {
		jcodec = NULL;
		jcodec = NCJith(jcodecs,k);
	        if(jcodec == NULL) break; /* done */
		if(NCJsort(jcodec) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
		if((stat = NCZ_filter_build(file,var,jcodec,chainindex++))) goto done;
	    }
	    /* Suppress variable if there are filters and var is not fixed-size */
   	    if(varsized && nclistlength((NClist*)var->filters) > 0) suppress = 1;
#endif
	}

	if(rank > 0) {
    	    if((stat = NCZ_computedimrefs(file, var, jvar, dimnames, shapes, var->dim))) goto done;
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

        if(suppress) {
	    /* Reclaim NCZarr variable specific info */
	    (void)NCZ_zclose_var1(var);
	    /* Remove from list of variables and reclaim the top level var object */
	    (void)nc4_var_list_del(grp, var);
	    var = NULL;
	}

	/* Clean up from last cycle */
	nclistfreeall(dimnames); dimnames = NULL;
        nullfree(varpath); varpath = NULL;
        nullfree(shapes); shapes = NULL;
        nullfree(key); key = NULL;
        NCJreclaim(jvar); jvar = NULL;
        var = NULL;
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

    /* Recurse to fill in subgroups */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = read_grp(file,g)))
	    goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Extract attributes from storage group or var and return
          the corresponding atypes list and the attributes json.
@param jblock - [in] the container's dict
@param jattsp - [out] the json for the attributes dict
@param atypesp - [out] the attribute type list
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
NCZ_load_jatts(NCjson* jblock, NCjson** jattsp, NClist** atypesp)
{
    int stat = NC_NOERR;
    NCjson* jatts = NULL;
    NCjson* jncatt = NULL;
    NClist* atypes = NULL; /* envv list */

    ZTRACE(3,"jblock=%p",jblock);

    /* alway return (possibly empty) list of types */
    atypes = nclistnew();

    /* Extract the attributes key value: may not exist */
    if((stat = NCJdictget(jblock,"attributes",&jatts))) goto done;
    if(jatts == NULL) goto exit; /* empty */

    /* Extract the nczarr key value: may not exist */
    if((stat = NCJdictget(jblock,NCZ_V3_ATTR,&jncatt))) goto done;

    if(jncatt != NULL) {
        NCjson* jtypes = NULL;
        /* jncatt attribute should be a dict */
        if(NCJsort(jncatt) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
        /* Extract "types; may not exist if only hidden attributes are defined */
        if((stat = NCJdictget(jncatt,"types",&jtypes))) goto done;
        if(jtypes != NULL) {
            if(NCJsort(jtypes) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
            /* Convert to an envv list */
            if((stat = NCZ_jtypes2atypes(jtypes,atypes))) goto done;
        }
    }
exit:
    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(atypesp) {*atypesp = atypes; atypes = NULL;}

done:
    if(stat) {
	nclistfreeall(atypes);
    }
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Potentially shared functions */

/**
 * @internal JSONize dims in a group
 *
 * @param  file pointer to file struct
 * @param  parent group
 * @param jdimsp return the array of jsonized dimensions
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NCjson** jdimsp)
{
    int i, stat=NC_NOERR;
    NCjson* jdims = NULL;
    NCjson* jdim = NULL;
    NCjson* jdimname = NULL;
    NCjson* jdimsize = NULL;
    NCjson* jdimunlimited = NULL;
    NCjson* jcfg = NULL;
    char slen[64];

    ZTRACE(3,"file=%s parent=%s",file->controller->path,parent->hdr.name);

    NCJnew(NCJ_ARRAY,&jdims);

    for(i=0; i<ncindexsize(parent->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(parent->dim,i);
        jdimsize = NULL;
	jdimname = NULL;
	jdimunlimited = NULL;
        jdim = NULL;
	jcfg = NULL;
	
        /* Collect the dimension info */

	/* jsonize dim name */
	NCJnewstring(NCJ_STRING,dim->hdr.name,&jdimname);
        /* Jsonize the size */
        snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
	NCJnewstring(NCJ_INT,slen,&jdimsize);
        /* And the unlimited flag*/
	if(dim->unlimited) {
	    NCJnewstring(NCJ_INT,"1",&jdimunlimited);
	}

	/* Assemble the dimension dict */
	
	/* Create configuration dict */
        NCJnew(NCJ_DICT,&jcfg);
	NCJinsert(jcfg,"size",jdimsize);
	jdimsize = NULL;
	if(jdimunlimited != NULL) NCJinsert(jcfg,"unlimited",jdimunlimited);
        jdimunlimited = NULL;

	/* Create dim dict */
        NCJnew(NCJ_DICT,&jdim);

	/* Add the dimension name*/
	NCJinsert(jdim,"name",jdimname);
	jdimname = NULL;

	/* Add the configuration */
	NCJinsert(jdim,"configuration",jcfg);
	jcfg = NULL;

	/* Add to jdims */
	NCJappend(jdims,jdim);
	jdim = NULL;
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}

    NCJreclaim(jdims);
    NCJreclaim(jdim);
    NCJreclaim(jdimsize);
    NCJreclaim(jdimname);
    NCJreclaim(jdimunlimited);
    NCJreclaim(jcfg);
    return ZUNTRACE(THROW(stat));
}


/**
 * @internal JSONize arrays in a group
 *
 * @param  file pointer to file struct
 * @param  parent group
 * @param jarrayp return the array of jsonized arrays
 * @return ::NC_NOERR No error.
 *
 * @author Dennis Heimbigner
 */
static int
NCZ_collect_arrays(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NCjson** jarraysp)
{
    int i, stat=NC_NOERR;
    NCjson* jarrays = NULL;
    NCjson* jarray = NULL;
    NCjson* jarrayname = NULL;

    ZTRACE(3,"file=%s parent=%s",file->controller->path,parent->hdr.name);

    NCJnew(NCJ_ARRAY,&jarrays);

    for(i=0; i<ncindexsize(parent->vars); i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(parent->vars,i);
	jarrayname = NULL;
	
	/* jsonize array name */
	NCJnewstring(NCJ_STRING,var->hdr.name,&jarrayname);

	/* Create array dict */
        NCJnew(NCJ_DICT,&jarray);

	/* Add the array name*/
	NCJinsert(jarray,"name",jarrayname);
	jarrayname = NULL;

	/* Add to jarrays */
	NCJappend(jarrays,jarray);
	jarray = NULL;
    }
    if(jarraysp) {*jarraysp = jarrays; jarrays = NULL;}

    NCJreclaim(jarrays);
    NCJreclaim(jarray);
    NCJreclaim(jarrayname);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal JSONize subgrps in a group
 *
 * @param  file pointer to file struct
 * @param  grp the group to jsonize
 * @param  jgrpp return the array of jsonized grps
 * @return ::NC_NOERR No error.
 *
 * @author Dennis Heimbigner
 */
static int
NCZ_collect_grps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jsubgrpsp)
{
    int i, stat=NC_NOERR;
    NCjson* jsubgrps = NULL;
    NCjson* jsubgrp = NULL;
    NCjson* jgrpname = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    /* Array of subgroup dicts */
    NCJnew(NCJ_ARRAY, &jsubgrps);

    /* For each subgroup, build the subgroup metadata and insert into
       the json for the group here. This is recursive.
    */
    for(i=0; i<ncindexsize(grp->children); i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
        /* Recurse to build the subgroup dict for g*/
	if((stat = build_group_json(file,g,&jsubgrp))) goto done;
	NCJappend(jsubgrps,jsubgrp);
	jsubgrp = NULL;
    }

    if(jsubgrpsp) {*jsubgrpsp = jsubgrps; jsubgrps = NULL;}
done:
    NCJreclaim(jsubgrp);
    NCJreclaim(jsubgrps);
    NCJreclaim(jgrpname);
    return ZUNTRACE(THROW(stat));
}


/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
@param file - [in] the containing file
@param zfile - [in] the containing file's annotation
@param container - [in] the containing object
@param jblock - [in] the container's metadata
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
NCZ_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jblock)
{
    int stat = NC_NOERR;
    int i;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_ATT_INFO_T* att = NULL;
    NCindex* attlist = NULL;
    NClist* atypes = NULL;
    NCjson* jattrs = NULL;
    nc_type typeid;
    size_t len, typelen;
    void* data = NULL;
    NC_ATT_INFO_T* fillvalueatt = NULL;
    nc_type typehint = NC_NAT;
    int purezarr;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;
 
    if(container->sort == NCGRP) {	
	grp = ((NC_GRP_INFO_T*)container);
	attlist =  grp->att;
    } else {
	var = ((NC_VAR_INFO_T*)container);
        zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
	attlist =  var->att;
    }

    switch ((stat = NCZ_load_jatts(jblock, &jattrs, &atypes))) {
    case NC_NOERR: break;
    case NC_EEMPTY:  /* container has no attributes */
        stat = NC_NOERR;
	break;
    default: goto done; /* true error */
    }

    if(jattrs != NULL) {
	/* Iterate over the attributes to create the in-memory attributes */
	/* Watch for special cases: _FillValue  etc. */
	for(i=0;i<NCJlength(jattrs);i+=2) {
	    NCjson* key = NCJith(jattrs,i);
	    NCjson* value = NCJith(jattrs,i+1);
	    const NC_reservedatt* ra = NULL;
	    int isfillvalue = 0;
    	    int isdfaltmaxstrlen = 0;
       	    int ismaxstrlen = 0;
	    const char* aname = NCJstring(key);
	    /* See if this is a notable attribute */
	    if(var != NULL && strcmp(aname,NC_ATT_FILLVALUE)==0) isfillvalue = 1;
	    if(grp != NULL && grp->parent == NULL && strcmp(aname,NC_NCZARR_DEFAULT_MAXSTRLEN_ATTR)==0)
	        isdfaltmaxstrlen = 1;
	    if(var != NULL && strcmp(aname,NC_NCZARR_MAXSTRLEN_ATTR)==0)
	        ismaxstrlen = 1;

	    /* See if this is reserved attribute */
	    ra = NC_findreserved(aname);
	    if(ra != NULL) {
		/* case 1: name = _NCProperties, grp=root, varid==NC_GLOBAL */
		if(strcmp(aname,NCPROPS)==0 && grp != NULL && file->root_grp == grp) {
		    /* Setup provenance */
		    if(!NCJisatomic(value))
			{stat = (THROW(NC_ENCZARR)); goto done;} /*malformed*/
		    if((stat = NCZ_read_provenance(file,aname,NCJstring(value))))
			goto done;
		}
		/* case other: if attribute is hidden */
		if(ra->flags & HIDDENATTRFLAG) continue; /* ignore it */
	    }
	    typehint = NC_NAT;
	    if(isfillvalue)
	        typehint = var->type_info->hdr.id ; /* if unknown use the var's type for _FillValue */
	    /* Create the attribute */
	    /* Collect the attribute's type and value  */
	    if((stat = NCZ_computeattrinfo(aname,atypes,typehint,purezarr,value,
				   &typeid,&typelen,&len,&data)))
		goto done;
	    if((stat = ncz_makeattr(container,attlist,aname,typeid,len,data,&att)))
		goto done;
	    /* No longer need this copy of the data */
   	    if((stat = NC_reclaim_data_all(file->controller,att->nc_typeid,data,len))) goto done;	    	    
	    data = NULL;
	    if(isfillvalue)
	        fillvalueatt = att;
	    if(ismaxstrlen && att->nc_typeid == NC_INT)
	        zvar->maxstrlen = ((int*)att->data)[0];
	    if(isdfaltmaxstrlen && att->nc_typeid == NC_INT)
	        zfile->default_maxstrlen = ((int*)att->data)[0];
	}
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue((NC_VAR_INFO_T*)container)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
	((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
	((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    if(data != NULL)
        stat = NC_reclaim_data(file->controller,att->nc_typeid,data,len);
    NCJreclaim(jattrs);
    nclistfreeall(atypes);
    return ZUNTRACE(THROW(stat));
}

static int
subobjects_pure(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* grpnames)
{
    int i,stat = NC_NOERR;
    char* grpkey = NULL;
    char* objkey = NULL;
    char* zjkey = NULL;
    char* content = NULL;
    NClist* matches = nclistnew();
    NCjson* json = NULL;
    NCjson* jnodetype = NULL; /* do not reclaim */

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_search(zfile->map,grpkey,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
	size64_t size;
	const char* name = nclistget(matches,i);
        nullfree(content); content = NULL;
	NCJreclaim(json); json = NULL;
	if(strcmp(name,Z3GROUP)==0) continue; /* only want next level */
	/* See if name/zarr.json exists */
	if((stat = nczm_concat(grpkey,name,&objkey))) goto done;
	if((stat = nczm_concat(objkey,Z3ARRAY,&zjkey))) goto done;
	if((stat = nczmap_len(zfile->map,zjkey,&size)) == NC_NOERR) {
	    /* Read contents of the zarr.json */
	    if((content = (char*)malloc(size))==NULL) {stat = NC_ENOMEM; goto done;}
	    if((stat = nczmap_read(zfile->map,zjkey,0,size,content))) goto done;
	    /* Parse */
	    if((NCJparse(content,0,&json))) {stat = NC_ENOTZARR; goto done;}
	    if(NCJsort(json) != NCJ_DICT) {stat = NC_ENOTZARR; goto done;}
	    if((NCJdictget(json,"node_type",&jnodetype))) {stat = NC_ENOTZARR; goto done;}
	    if(strcasecmp("group",NCJstring(jnodetype))==0)
	        nclistpush(grpnames,strdup(NCJstring(jnodetype)));
	    else if(strcasecmp("array",NCJstring(jnodetype))==0)
	        nclistpush(varnames,strdup(NCJstring(jnodetype)));
	    /* else ignore */
	}
	stat = NC_NOERR;
    }

done:
    nullfree(objkey);
    nullfree(zjkey);
    nullfree(content);
    nclistfreeall(matches);
    NCJreclaim(json);
    return stat;
}
static int
subobjects(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* grpnames)
{
    int i;
    NCZ_GRP_INFO_T* zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;
    NCjson* jgrp = zgrp->grpsuper;
    NCjson* jarrays = NULL;
    NCjson* jsubgrps = NULL;

    NCJdictget(jgrp,"arrays",&jarrays);
    NCJdictget(jgrp,"children",&jsubgrps);

    for(i=0;i<NCJlength(jarrays);i++) {
	NCjson* jarray = NCJith(jarrays,i);
	NCjson* jname = NULL;
	assert(NCJsort(jarray)==NCJ_DICT);
	NCJdictget(jarray,"name",&jname);
	assert(NCJisatomic(jname));
        nclistpush(varnames,strdup(NCJstring(jname)));
    }
    for(i=0;i<NCJlength(jsubgrps);i++) {
	NCjson* jsubgrp = NCJith(jsubgrps,i);
	NCjson* jname = NULL;
	assert(NCJsort(jsubgrp)==NCJ_DICT);
	NCJdictget(jsubgrp,"name",&jname);
	assert(NCJisatomic(jname));
        nclistpush(varnames,strdup(NCJstring(jname)));
    }
    return NC_NOERR;
}

/* Convert a list of integer strings to 64 bit dimension sizes (shapes) */
static int
NCZ_decodeints(NCjson* jshape, size64_t* shapes)
{
    int i, stat = NC_NOERR;

    for(i=0;i<NCJlength(jshape);i++) {
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

/*
Extract type and data for an attribute
*/
static int
NCZ_computeattrinfo(const char* name, NClist* atypes, nc_type typehint, int purezarr, NCjson* values,
		nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap)
{
    int stat = NC_NOERR;
    int i;
    size_t len;
    void* data = NULL;
    nc_type typeid = 0;
    int typelen = 0;

    ZTRACE(3,"name=%s |atypes|=%u typehint=%d purezarr=%d values=|%s|",name,nclistlength(atypes),typehint,purezarr,NCJtotext(values));

    /* Get type info for the given att */
    typeid = NC_NAT;
    for(i=0;i<nclistlength(atypes);i+=2) {
	const char* aname = nclistget(atypes,i);
	if(strcmp(aname,name)==0) {
	    const char* atype = nclistget(atypes,i+1);
	    if((stat = ncz3_dtype2nctype(atype,purezarr,&typeid,&typelen))) goto done;
	    break;
	}
    }
    if(typeid > NC_MAX_ATOMIC_TYPE)
	{stat = NC_EINTERNAL; goto done;}
    /* Use the hint if given one */
    if(typeid == NC_NAT)
        typeid = typehint;

    if((stat = NCZ_computeattrdata(typehint, &typeid, values, (size_t*)&typelen, &len, &data))) goto done;

    if(typeidp) *typeidp = typeid;
    if(lenp) *lenp = len;
    if(typelenp) *typelenp = typelen;
    if(datap) {*datap = data; data = NULL;}

done:
    nullfree(data);
    return ZUNTRACEX(THROW(stat),"typeid=%d typelen=%d len=%u",*typeidp,*typelenp,*lenp);
}

/*
Extract data for an attribute
*/
static int
NCZ_computeattrdata(nc_type typehint, nc_type* typeidp, NCjson* values, size_t* typelenp, size_t* countp, void** datap)
{
    int stat = NC_NOERR;
    NCbytes* buf = ncbytesnew();
    size_t typelen;
    nc_type typeid = NC_NAT;
    NCjson* jtext = NULL;
    int reclaimvalues = 0;
    int isjson = 0; /* 1 => attribute value is neither scalar nor array of scalars */
    int count = 0; /* no. of attribute values */

    ZTRACE(3,"typehint=%d typeid=%d values=|%s|",typehint,*typeidp,NCJtotext(values));

    /* Get assumed type */
    if(typeidp) typeid = *typeidp;
    if(typeid == NC_NAT && !isjson) {
        if((stat = NCZ_inferattrtype(values,typehint, &typeid))) goto done;
    }

    /* See if this is a simple vector (or scalar) of atomic types */
    isjson = NCZ_iscomplexjson(values,typeid);

    if(isjson) {
	/* Apply the JSON attribute convention and convert to JSON string */
	typeid = NC_CHAR;
	if((stat = NCZ_json_convention_read(values,&jtext))) goto done;
	values = jtext; jtext = NULL;
	reclaimvalues = 1;
    } 

    if((stat = NC4_inq_atomic_type(typeid, NULL, &typelen)))
        goto done;

    /* Convert the JSON attribute values to the actual netcdf attribute bytes */
    if((stat = NCZ_attr_convert(values,typeid,typelen,&count,buf))) goto done;

    if(typelenp) *typelenp = typelen;
    if(typeidp) *typeidp = typeid; /* return possibly inferred type */
    if(countp) *countp = count;
    if(datap) *datap = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    if(reclaimvalues) NCJreclaim(values); /* we created it */
    return ZUNTRACEX(THROW(stat),"typelen=%d count=%u",(typelenp?*typelenp:0),(countp?*countp:-1));
}

/* Compute the set of dim refs for this variable, taking purezarr into account */
static int
NCZ_computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jvar, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int stat = NC_NOERR;
    int i;
    int purezarr = 0;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jdimnames = NULL;
    NCjson* jshape = NULL;
    NCjson* jdimfqns = NULL;
    NCjson* jnczarray = NULL;
    int ndims;
    NC_DIM_INFO_T* vardims[NC_MAX_VAR_DIMS];

    ZTRACE(3,"file=%s var=%s purezarr=%d ndims=%d shape=%s",
    	file->controller->path,var->hdr.name,purezarr,(int)ndims,nczprint_vector(ndims,shapes));

    if(zfile->controls.flags & FLAG_PUREZARR) purezarr = 1;

    assert(var->atts_read);

    if(dims) *dims = NULL; 
    ndims = var->ndims;
    if(ndims == 0) goto done; /* scalar */

    /* We have two sources for the dimension names for this variable.
	1. the "dimension_names" inside the zarr.json dictionary.
	2. the "dimensions" key inside the _nczarr_array dictionary.

	If purezarr, then we only have #1. In that case, for each name in "dimension_names",
	we need to do the following:
	1. get the corresponding size from the "shapes" vector.
	2. if the name is null, then set the name to "_zdim_<n>",
	   where n is the size from the shape vector.
	3. search up the containing parent groups looking for a dimension
	   with that name.
	4. if found, then use that dimension.
	5. if not found, then create the dimension in the immediately containing group.

	If not purezarr, then we verify for each FQN in #2 that
	1. its simple name (the last segment of the FQN) is the same as the name from #1.
	2. we use the FQN to find the actual dimension.
    */   

    /* Get the shape vector */
    NCJdictget(jvar,"shape",&jshape);
    assert(jshape != NULL && NCJsort(jshape)==NCJ_ARRAY && NCJlength(jshape)==ndims);

    /* Get the dimension_names array */
    NCJdictget(jvar,"dimension_names",&jdimnames);

    if(!purezarr) {
	NCJdictget(jvar,NCZ_V3_ARRAY,&jnczarray);
	if(jnczarray == NULL) {stat = NC_ENCZARR; goto done;}
        /* get the FQNS */
	NCJdictget(jnczarray,"dimensions",&jdimfqns);
	if(jdimfqns == NULL) {stat = NC_ENCZARR; goto done;}
	assert(jdimfqns != NULL && NCJlength(jdimfqns) == ndims);
    }

    if(purezarr) {
	for(i=0;i<ndims;i++) {
	    char pseudodim[64];
	    size64_t size;
	    char* dimname = NULL;
	    NCjson* jdimname = NULL;
	    NCjson* jsize = NULL;
	    NC_DIM_INFO_T* dim = NULL;

	    jsize = NCJith(jshape,i);
	    assert(jsize != NULL && NCJsort(jsize)==NCJ_INT);
	    sscanf(NCJstring(jsize),"%llu",&size);
	    if(jdimnames != NULL && i < NCJlength(jdimnames)) {
	        jdimname = NCJith(jdimnames,i);
		if(jdimname != NULL && NCJsort(jdimname) == NCJ_NULL) jdimname = NULL;
	    }
	    if(jdimname == NULL) {
		snprintf(pseudodim,sizeof(pseudodim),"_zdim_%llu",size);
		dimname = pseudodim;
	    } else
	        dimname = NCJstring(jdimname);
	    if((stat = finddim(file,var->container,dimname,size,&dim))) goto done;
	    if(dim == NULL) {
		/* Create dim in this grp */
		if((stat = nc4_dim_list_add(var->container, dimname, (size_t)size, -1, &dim))) goto done;
	    }
	    assert(dim != NULL);
	    vardims[i] = dim;
	}
    } else { /* !purezarr */
	for(i=0;i<ndims;i++) {
	    NC_DIM_INFO_T* dim = NULL;
	    NCjson* jfqn = NCJith(jdimfqns,i);
	    NCjson* jdimname = NULL;
	    if(jdimnames != NULL && i < NCJlength(jdimnames)) {
	        jdimname = NCJith(jdimnames,i);
		if(jdimname != NULL && NCJsort(jdimname) == NCJ_NULL) jdimname = NULL;
	    }	
	    if(jdimname != NULL) {
		char* fqn = NCJstring(jfqn);
		/* if the dimension_name name is not null, then verify against the fqn */
		char* p = strrchr(fqn,'/');
		if(p == NULL) p = fqn; else p++; /* point to name, not any leading '/' */
		if(strcmp(p,NCJstring(jdimname))!=0) {stat = NC_ENCZARR; goto done;} /* verify name match */		
		/* Find the dimension matching the fqn */
		if((stat = NCZ_locateFQN(var->container,p,NCDIM,(NC_OBJ**)&dim))) goto done;
		assert(dim != NULL);
		vardims[i] = dim;
	    }
	}
    }
    for(i=0;i<ndims;i++) {if(vardims[i]==NULL) {stat = NC_EBADDIM; goto done;}}
    /* add the dimensions to the variable */
    nc4_var_set_ndims(var,ndims);
    /* There should be a function for this */
    assert(var->dim && var->dimids);
    for(i=0;i<ndims;i++) {
        var->dim[i] = vardims[i];
	var->dimids[i] = vardims[i]->hdr.id;
    }
    
done:
    return ZUNTRACE(THROW(stat));
}

/**
Implement the JSON convention:
Stringify it as the value and make the attribute be of type "char".
*/

static int
NCZ_json_convention_read(NCjson* json, NCjson** jtextp)
{
    int stat = NC_NOERR;
    NCjson* jtext = NULL;
    char* text = NULL;

    if(json == NULL) {stat = NC_EINVAL; goto done;}
    if(NCJunparse(json,0,&text)) {stat = NC_EINVAL; goto done;}
    if(NCJnewstring(NCJ_STRING,text,&jtext)) {stat = NC_EINVAL; goto done;}
    *jtextp = jtext; jtext = NULL;
done:
    NCJreclaim(jtext);
    nullfree(text);
    return stat;
}

/* Convert a json value to actual data values of an attribute. */
static int
NCZ_attr_convert(NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst)
{
    int stat = NC_NOERR;
    int i;
    int count = 0;
    
    ZTRACE(3,"src=%s typeid=%d typelen=%u",NCJtotext(src),typeid,typelen);
	    
    /* 3 cases:
       (1) singleton atomic value
       (2) array of atomic values
       (3) other JSON expression
    */
    switch (NCJsort(src)) {
    case NCJ_INT: case NCJ_DOUBLE: case NCJ_BOOLEAN: /* case 1 */
	count = 1;
	if((stat = NCZ_convert1(src, typeid, dst)))
	    goto done;
	break;

    case NCJ_ARRAY:
        if(typeid == NC_CHAR) {
	    if((stat = NCZ_charify(src,dst))) goto done;
	    count = ncbyteslength(dst);
        } else {
	    count = NCJlength(src);
	    for(i=0;i<count;i++) {
	        NCjson* value = NCJith(src,i);
                if((stat = NCZ_convert1(value, typeid, dst))) goto done;
	    }
	}
	break;
    case NCJ_STRING:
	if(typeid == NC_CHAR) {
	    if((stat = NCZ_charify(src,dst))) goto done;
	    count = ncbyteslength(dst);
	    /* Special case for "" */
	    if(count == 0) {
	        ncbytesappend(dst,'\0');
	        count = 1;
	    }
	} else {
	    if((stat = NCZ_convert1(src, typeid, dst))) goto done;
	    count = 1;
	}
	break;
    default: stat = (THROW(NC_ENCZARR)); goto done;
    }
    if(countp) *countp = count;

done:
    return ZUNTRACE(THROW(stat));
}

#if 0
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
#endif

/* Convert an attribute "types list to an envv style list */
static int
NCZ_jtypes2atypes(NCjson* jtypes, NClist* atypes)
{
    int i, stat = NC_NOERR;
    for(i=0;i<NCJlength(jtypes);i+=2) {
	const NCjson* key = NCJith(jtypes,i);
	const NCjson* value = NCJith(jtypes,i+1);
	if(!NCJisatomic(key)) {stat = (THROW(NC_ENCZARR)); goto done;}
	if(!NCJisatomic(value)) {stat = (THROW(NC_ENCZARR)); goto done;}
	nclistpush(atypes,strdup(NCJstring(key)));
	nclistpush(atypes,strdup(NCJstring(value)));
    }
done:
    return stat;
}

/* Convert a JSON singleton or array of strings to a single string */
static int
NCZ_charify(NCjson* src, NCbytes* buf)
{
    int i, stat = NC_NOERR;
    struct NCJconst jstr = NCJconst_empty;

    if(NCJsort(src) != NCJ_ARRAY) { /* singleton */
        if((stat = NCJcvt(src, NCJ_STRING, &jstr))) goto done;
        ncbytescat(buf,jstr.sval);
    } else for(i=0;i<NCJlength(src);i++) {
	NCjson* value = NCJith(src,i);
	if((stat = NCJcvt(value, NCJ_STRING, &jstr))) goto done;
	ncbytescat(buf,jstr.sval);
        nullfree(jstr.sval);jstr.sval = NULL;
    }
done:
    nullfree(jstr.sval);
    return stat;
}

/*
Search up the parent tree looking for a dimension by name.
*/
static int
finddim(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* dimname, size64_t size, NC_DIM_INFO_T** dimp)
{
    int stat = NC_NOERR;
    NC_DIM_INFO_T* dim = NULL;

    dim = (NC_DIM_INFO_T*)ncindexlookup(grp->dim,dimname);
    if(dim == NULL && grp->parent != NULL)
	{if((stat = finddim(file,grp->parent,dimname,size,&dim))) goto done;}
    if(dimp) *dimp = dim;    
done:
    return THROW(stat);
}


/**************************************************/
/* Format Filter Support Functions */

/* JSON Parse/unparse of filter codecs */

#ifdef ENABLE_NCZARR_FILTERS
int
ZF3_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;

    /* Convert the HDF5 id + visible parameters to the codec form */

    /* Clear any previous codec */
    nullfree(filter->codec.id); filter->codec.id = NULL;
    nullfree(filter->codec.codec); filter->codec.codec = NULL;
    filter->codec.id = strdup(filter->plugin->codec.codec->codecid);
    if(filter->plugin->codec.codec->NCZ_hdf5_to_codec) {
	stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(NCplistzarrv3,filter->hdf5.id,filter->hdf5.visible.nparams,filter->hdf5.visible.params,&filter->codec.codec);
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
ZF3_codec2hdf(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter, NCZ_Plugin* plugin)
{
    int stat = NC_NOERR;
    NCjson* jvalue = NULL;
    
    assert(jfilter != NULL);
    assert(filter != NULL);
    
    if(filter->codec.id == NULL) {
        /* Get the id of this codec filter */
        if(NCJdictget(jfilter,"name",&jvalue)) {stat = NC_EFILTER; goto done;}
        if(!NCJisatomic(jvalue)) {stat = THROW(NC_ENOFILTER); goto done;}
        filter->codec.id = strdup(NCJstring(jvalue));
    }
    
    if(plugin != NULL) {
	/* Save the hdf5 id */
	filter->hdf5.id = plugin->hdf5.filter->id;
	/* Convert the codec to hdf5 form visible parameters */
        if(plugin->codec.codec->NCZ_codec_to_hdf5) {
            stat = plugin->codec.codec->NCZ_codec_to_hdf5(NCplistzarrv3,filter->codec.codec,&filter->hdf5.id,&filter->hdf5.visible.nparams,&filter->hdf5.visible.params);
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

static const NCZ_Formatter NCZ_formatter3_table = {
    NCZARRFORMAT1,
    ZARRFORMAT3,
    NCZ_FORMATTER_VERSION,

    ZF3_create,
    ZF3_open,
    ZF3_close,
    ZF3_readmeta,
    ZF3_writemeta,
    ZF3_readattrs,
    ZF3_buildchunkkey,
#ifdef ENABLE_NCZARR_FILTERS
    ZF3_codec2hdf,
    ZF3_hdf2codec,
#else
    NULL,
    NULL,
#endif
};

const NCZ_Formatter* NCZ_formatter3 = &NCZ_formatter3_table;

int
NCZF3_initialize(void)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;
    if(NCJparse(NCZ_Bytes_Little_Text,0,&json) < 0) {stat = NC_EINTERNAL; goto done;}
    NCZ_Bytes_Little_Json = json;
    if(NCJparse(NCZ_Bytes_Big_Text,0,&json) < 0) {stat = NC_EINTERNAL; goto done;}
    NCZ_Bytes_Big_Json = json;
done:
    return THROW(stat);
}

int
NCZF3_finalize(void)
{
    return NC_NOERR;
}

