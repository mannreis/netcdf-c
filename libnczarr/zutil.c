/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
 * @file
 * @internal Misc. utility code
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"

#undef DEBUG

/**************************************************/
/**
Type Issues:

There are (currently) two type issues that need special hacks.
1. Need dtypes to distinquish NC_STRING && MAXSTRLEN==1
   (assuming fixed size strings) from NC_CHAR in a way
   that allows other Zarr implementations to read the data.
2. Need a fake dtype to support the JSON convention allowing
   an attribute's value to be a JSON value.

Zarr Version 2:
-------------------
For issue 1, use these dtypes to distinquish NC_STRING && MAXSTRLEN==1 from NC_CHAR
* ">S1" for NC_CHAR.
* "|S1" for NC_STRING && MAXSTRLEN==1
* "|Sn" for NC_STRING && MAXSTRLEN==n
Note that we could use "|U1", but since this is utf-16 or utf-32
in python, it may cause problems when reading what amounts to utf-8.

For issue 2, use this type to identify a JSON valued attribute.
* "|J0"

These choices are admittedly a bit of a hack, and the first case in particular
will probably cause errors in some other Zarr implementations; the Zarr spec
is unclear about what combinations are legal. Issue 2 will only be interpreted by
NCZarr code, so that choice is arbitrary.

Zarr Version 3:
-------------------
For issues 1 and 2, we have the following table:
| dtype | type_alias |
| ----- | ---------- |
| uint8 | char       |
| rn    | string     |
| uint8 | json       |


In the event that we are reading a pure Zarr file, we need to make
inferences about the above issues but lacking any NCZarr hints.

First, we need to define a rule to define what attribute values can be
considered a "complex" json expression. So when we read the JSON
value of an attribute, that value is classified as complex or simple.
Simple valued attributes will be mapped to atomic-valued
netcdf attributes.  Complex valued attributes are "unparsed" to a
string and the attribute is stored as an NC_CHAR typed attribute.

The current rule for defining a complex JSON valued attribute is defined
by the function NCZ_iscomplexjson(). Basically the rule is as follows:
1. If the attribute value is a single atomic value or NULL or a JSON array
   of atomic values, then the attribute value is SIMPLE.
2. Otherwise, the attribute value is COMPLEX.

In the event that we want to write a complex JSON valued attribute,
we use the following rules in order (see NCZ_iscomplexjsontext()):
1. Attribute type is not of type NC_CHAR => not complex
2. Attribute value contains no unescaped '[' and no unescaped '{' => not complex
3. The value, treated as a string, is not JSON parseable => notcomplex
4. else the value can be treated as a complex json value.

This is admittedly a hack that uses rule 2 to delay parsing the
attribute value as long as possible. Note the rules will change
when/if structured types (e.g. compound, complex) are added.

Assuming the attribute value is not a complex JSON expression, we assume
the value is a single atomic value or an array of atomic values.

We infer the type -- see NCZ_inferattrtype() -- by looking at the
first (possibly only) value of the attribute. The only tricky part of this
occurs when we have a JSON string value. We need to decide if the type
should be treated as NC_CHAR or as NC_STRING.
The current rules are as follows:
1. choose NC_CHAR if:
    a. The value is a single value (not a JSON array) and NCJsort == NCJ_STRING
    b. The value is an array and each element of the array
       is a single character.
2. else choose NC_STRING.

So, for example:
* "a" => NC_CHAR
* "abcdef" => NC_CHAR
* ["abcdef"] => NC_STRING
* ["a","b","c","d","e","f"] => NC_CHAR
* ["abc", "def"] => NC_STRING

*/

/* Static zarr type name table */
/* Used to convert nc_type <=> dtype */
static const struct ZTYPESV2 {
    const char* dtype;
    int typelen;    
} znamesv2[N_NCZARR_TYPES] = {
/* nc_type       dtype */
/*NC_NAT*/	{NULL,0},
/*NC_BYTE*/	{"|i1",1},
/*NC_CHAR*/	{">S1",1},
/*NC_SHORT*/	{"|i2",2},
/*NC_INT*/	{"|i4",4},
/*NC_FLOAT*/	{"|f4",4},
/*NC_DOUBLE*/	{"|f8",8},
/*NC_UBYTE*/	{"|u1",1},
/*NC_USHORT*/	{"|u2",2},
/*NC_UINT*/	{"|u4",4},
/*NC_INT64*/	{"|i8",8},
/*NC_UINT64*/	{"|u8",8},
/*NC_STRING*/	{"|S%d",0},
/*NC_JSON*/	{"|J0",0} /* NCZarr internal type */
};

static const struct ZTYPESV3 {
    const char* zarr; /* Must be a legitimate Zarr V3 type */
    size_t typelen;
    const char* type_alias;
} znamesv3[N_NCZARR_TYPES] = {
/* nc_type       Pure Zarr   typelen    NCZarr */
/*NC_NAT*/      {NULL,          0,      NULL},
/*NC_BYTE*/     {"int8",        0,      NULL},
/*NC_CHAR*/     {"uint8",       0,      "char"},
/*NC_SHORT*/    {"int16",       0,      NULL},
/*NC_INT*/      {"int32",       0,      NULL},
/*NC_FLOAT*/    {"float32",     0,      NULL},
/*NC_DOUBLE*/   {"float64",     0,      NULL},
/*NC_UBYTE*/    {"uint8",       0,      NULL},
/*NC_USHORT*/   {"uint16",      0,      NULL},
/*NC_UINT*/     {"uint32",      0,      NULL},
/*NC_INT64*/    {"int64",       0,      NULL},
/*NC_UINT64*/   {"uint64",      0,      NULL},
/*NC_STRING*/   {"r%u",         0,      "string"},
/*NC_JSON*/     {"uint8",       0,      "json"} /* NCZarr internal type */
};

/* map nc_type -> NCJ_SORT */
static int zjsonsort[N_NCZARR_TYPES] = {
NCJ_UNDEF, /*NC_NAT*/
NCJ_INT, /*NC_BYTE*/
NCJ_STRING, /*NC_CHAR*/
NCJ_INT, /*NC_SHORT*/
NCJ_INT, /*NC_INT*/
NCJ_DOUBLE, /*NC_FLOAT*/
NCJ_DOUBLE, /*NC_DOUBLE*/
NCJ_INT, /*NC_UBYTE*/
NCJ_INT, /*NC_USHORT*/
NCJ_INT, /*NC_UINT*/
NCJ_INT, /*NC_INT64*/
NCJ_INT, /*NC_UINT64*/
NCJ_STRING, /*NC_STRING*/
NCJ_DICT, /*NC_JSON*/
};

/* Forward */
static int splitfqn(const char* fqn0, NClist* segments);

/**************************************************/

/**
@internal Get key for a group
@param grp - [in] group
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_grpkey(const NC_GRP_INFO_T* grp, char** pathp)
{
    int stat = NC_NOERR;
    NClist* segments = nclistnew();
    NCbytes* path = NULL;
    NC_GRP_INFO_T* parent = NULL;
    int i;

    nclistinsert(segments,0,(void*)grp);
    parent = grp->parent;
    while(parent != NULL) {
        nclistinsert(segments,0,parent);
        parent = parent->parent;
    }
    path = ncbytesnew();
    for(i=0;i<nclistlength(segments);i++) {
	grp = nclistget(segments,i);
	if(i > 1) ncbytescat(path,"/"); /* Assume root is named "/" */
	ncbytescat(path,grp->hdr.name);
    }        
    if(pathp) *pathp = ncbytesextract(path);

    nclistfree(segments);
    ncbytesfree(path);
    return stat;

}

/**
@internal Get key for a var
@param var - [in] var
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_varkey(const NC_VAR_INFO_T* var, char** pathp)
{
    int stat = NC_NOERR;
    char* grppath = NULL;
    char* varpath = NULL;

    /* Start by creating the full path for the parent group */
    if((stat = NCZ_grpkey(var->container,&grppath)))
	goto done;
    /* Create the suffix path using the var name */
    if((stat = nczm_concat(grppath,var->hdr.name,&varpath)))
	goto done;
    /* return path */
    if(pathp) {*pathp = varpath; varpath = NULL;}

done:
    nullfree(grppath);
    nullfree(varpath);
    return stat;
}

/**
@internal Get key for a dimension
@param dim - [in] dim
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_dimkey(const NC_DIM_INFO_T* dim, char** pathp)
{
    int stat = NC_NOERR;
    char* grppath = NULL;
    char* dimpath = NULL;

    /* Start by creating the full path for the parent group */
    if((stat = NCZ_grpkey(dim->container,&grppath)))
	goto done;
    /* Create the suffix path using the dim name */
    if((stat = nczm_concat(grppath,dim->hdr.name,&dimpath)))
	goto done;
    /* return path */
    if(pathp) {*pathp = dimpath; dimpath = NULL;}

done:
    nullfree(grppath);
    nullfree(dimpath);
    return stat;
}

/**
@internal Split a key into pieces along '/' character; elide any leading '/'
@param  key - [in]
@param segments - [out] split path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
ncz_splitkey(const char* key, NClist* segments)
{
    return nczm_split(key,segments);
}

/**************************************************/
/* Json sync code */

/**
@internal Down load a .z... structure into memory
@param zmap - [in] controlling zarr map
@param key - [in] .z... object to load
@param jsonp - [out] root of the loaded json
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_downloadjson(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    size64_t len;
    char* content = NULL;
    NCjson* json = NULL;

    if((stat = nczmap_len(zmap, key, &len)))
	goto done;
    if((content = malloc(len+1)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((stat = nczmap_read(zmap, key, 0, len, (void*)content)))
	goto done;
    content[len] = '\0';

    if(NCJparse(content,0,&json) < 0)
	{stat = NC_ENCZARR; goto done;}

    if(jsonp) {*jsonp = json; json = NULL;}

done:
    NCJreclaim(json);
    nullfree(content);
    return stat;
}

/**
@internal  Upload a modified json tree to a .z... structure.
@param zmap - [in] controlling zarr map
@param key - [in] .z... object to load
@param json - [in] root of the json tree
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_uploadjson(NCZMAP* zmap, const char* key, NCjson* json)
{
    int stat = NC_NOERR;
    char* content = NULL;

    ZTRACE(4,"zmap=%p key=%s",zmap,key);

#ifdef DEBUG
fprintf(stderr,"uploadjson: %s\n",key); fflush(stderr);
#endif
    /* Unparse the modified json tree */
    if(NCJunparse(json,0,&content)) goto done;
    ZTRACEMORE(4,"\tjson=%s",content);
    
if(getenv("NCS3JSON") != NULL)
fprintf(stderr,">>>> uploadjson: %s: %s\n",key,content);

    /* Write the metadata */
    if((stat = nczmap_write(zmap, key, strlen(content), content)))
	goto done;

done:
    nullfree(content);
    return ZUNTRACE(stat);
}

#if 0
/**
@internal create object, return empty dict; ok if already exists.
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EINVAL if object exists
@author Dennis Heimbigner
*/
int
NCZ_createdict(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    /* See if it already exists */
    stat = NCZ_downloadjson(zmap,key,&json);
    if(stat != NC_NOERR) {
	if(stat == NC_EEMPTY) {/* create it */
	    if((stat = nczmap_def(zmap,key,NCZ_ISMETA)))
		goto done;	    
        } else
	    goto done;
    } else {
	/* Already exists, fail */
	stat = NC_EINVAL;
	goto done;
    }
    /* Create the empty dictionary */
    NCJcheck(NCJnew(NCJ_DICT,&json));
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}

/**
@internal create object, return empty array; ok if already exists.
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EINVAL if object exits
@author Dennis Heimbigner
*/
int
NCZ_createarray(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    stat = NCZ_downloadjson(zmap,key,&json);
    if(stat != NC_NOERR) {
	if(stat == NC_EEMPTY) {/* create it */
	    if((stat = nczmap_def(zmap,key,NCZ_ISMETA)))
		goto done;	    
	    /* Create the initial array */
	    NCJnew(NCJ_ARRAY,&json);
        } else {
	    stat = NC_EINVAL;
	    goto done;
	}
    }
    if(json->sort != NCJ_ARRAY) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}
#endif /*0*/

/**
@internal Get contents of a meta object; fail it it does not exist
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EEMPTY [object did not exist]
@author Dennis Heimbigner
*/
int
NCZ_readdict(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    if((stat = NCZ_downloadjson(zmap,key,&json)))
	goto done;
    if(NCJsort(json) != NCJ_DICT) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}

/**
@internal Get contents of a meta object; fail it it does not exist
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json
@return NC_NOERR
@return NC_EEMPTY [object did not exist]
@author Dennis Heimbigner
*/
int
NCZ_readarray(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    if((stat = NCZ_downloadjson(zmap,key,&json)))
	goto done;
    if(NCJsort(json) != NCJ_ARRAY) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}

#if 0
/**
@internal Given an nc_type, produce the corresponding
default fill value as a string.
@param nctype - [in] nc_type
@param defaltp - [out] pointer to hold pointer to the value
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_default_fill_value(nc_type nctype, const char** dfaltp)
{
    if(nctype <= 0 || nctype > N_NCZARR_TYPES) return NC_EINVAL;
    if(dfaltp) *dfaltp = zfillvalue[nctype];
    return NC_NOERR;	        
}
#endif

/**
@internal Given an nc_type, produce the corresponding
fill value JSON type
@param nctype - [in] nc_type
@param sortp - [out] pointer to hold pointer to the JSON type
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_fill_value_sort(nc_type nctype, int* sortp)
{
    if(nctype <= 0 || nctype > N_NCZARR_TYPES) return NC_EINVAL;
    if(sortp) *sortp = zjsonsort[nctype];
    return NC_NOERR;	        
}

/* Return 1 if this machine is little endian */
int
NCZ_isLittleEndian(void)
{
    union {
        unsigned char bytes[SIZEOF_INT];
	int i;
    } u;
    u.i = 1;
    return (u.bytes[0] == 1 ? 1 : 0);
}


/*
Given a path to a group, return the list of objects
that contain another object with the name of the tag.
For example, we can get the immediate list of subgroups
by using the tag ".zgroup".
Basically we return the set of X where <prefix>/X/<tag>
is an object in the map.
Note: need to test with "/", "", and with and without trailing "/".
*/
int
NCZ_subobjects(NCZMAP* map, const char* prefix, const char* tag, char dimsep, NClist* objlist)
{
    int i,stat=NC_NOERR;
    NClist* matches = nclistnew();
    NCbytes* path = ncbytesnew();

    /* Get the list of names just below prefix */
    if((stat = nczmap_list(map,prefix,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
	const char* name = nclistget(matches,i);
	size_t namelen= strlen(name);	
	/* Ignore keys that start with .z or .nc or a potential chunk name */
	if(namelen >= 3 && name[0] == '.' && name[1] == 'n' && name[2] == 'c')
	    continue;
	if(namelen >= 2 && name[0] == '.' && name[1] == 'z')
	    continue;
	if(NCZ_ischunkname(name,dimsep))
	    continue;
	/* Create <prefix>/<name>/<tag> and see if it exists */
	ncbytesclear(path);
	ncbytescat(path,prefix);
	ncbytescat(path,"/");
	ncbytescat(path,name);
	ncbytescat(path,tag);
	/* See if this object exists */
        if((stat = nczmap_exists(map,ncbytescontents(path))) == NC_NOERR)
	    nclistpush(objlist,name);
    }

done:
    nclistfreeall(matches);
    ncbytesfree(path);
    return stat;
}

/**
@internal Zarr V2: Given an nc_type+endianness+purezarr+MAXSTRLEN, produce the corresponding Zarr dtype string.
@param nctype     - [in] nc_type
@param endianness - [in] endianness
@param purezarr   - [in] 1=>pure zarr, 0 => nczarr
@param len        - [in] max string length
@param dnamep     - [out] pointer to hold pointer to the dtype; user frees
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz2_nctype2dtype(nc_type nctype, int endianness, int purezarr, int len, char** dnamep)
{
    char dname[64];
    const char* dtype = NULL;

    if(nctype <= NC_NAT || nctype > N_NCZARR_TYPES) return NC_EINVAL;
    dtype = znamesv2[nctype].dtype;
    snprintf(dname,sizeof(dname),dtype,len);
    /* Set endianness */
    switch (nctype) {
    case NC_STRING:
    case NC_CHAR:
    case NC_JSON:
	break;    
    default:
	switch (endianness) {
	case NC_ENDIAN_LITTLE: dname[0] = '<'; break;
	case NC_ENDIAN_BIG: dname[0] = '>'; break;
	case NC_ENDIAN_NATIVE: default: break;
	}
    }
    if(dnamep) *dnamep = strdup(dname);
    return NC_NOERR;		
}

/*
@internal Convert a numcodecs Zarr v2 dtype spec to a corresponding nc_type.
@param nctype   - [in] dtype the dtype to convert
@param nctype   - [in] typehint help disambiguate e.g. char vs string
@param purezarr - [in] 1=>pure zarr, 0 => nczarr
@param nctypep  - [out] hold corresponding type
@param endianp  - [out] hold corresponding endianness
@param typelenp - [out] hold corresponding type size (for fixed length strings)
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz2_dtype2nctype(const char* dtype, nc_type typehint, int purezarr, nc_type* nctypep, int* endianp, size_t* typelenp)
{
    int stat = NC_NOERR;
    size_t typelen = 0;
    char tchar;
    nc_type nctype = NC_NAT;
    int endianness = -1;
    const char* p;
    int n,count;

    if(endianp) *endianp = NC_ENDIAN_NATIVE;
    if(nctypep) *nctypep = NC_NAT;
    if(typelenp) *typelenp = 0;

    if(dtype == NULL) {stat = NC_ENCZARR; goto done;}
    p = dtype;
    switch (*p++) {
    case '<': endianness = NC_ENDIAN_LITTLE; break;
    case '>': endianness = NC_ENDIAN_BIG; break;
    case '|': endianness = NC_ENDIAN_NATIVE; break;
    default: p--; endianness = NC_ENDIAN_NATIVE; break;
    }
    tchar = *p++; /* get the base type */
    /* Decode the type length */
    count = sscanf(p,"%zd%n",&typelen,&n);
    if(count == 0) {stat = NC_ENCZARR; goto done;}
    p += n;

    /* Short circuit special cases */
    if(tchar == 'J') {
        nctype = NC_JSON;
    } else if(tchar == 'S') {
	/* Fixed length string */
	if(endianness == NC_ENDIAN_BIG && typelen == 1)
	    nctype = NC_CHAR;
	else
	    nctype = NC_STRING;
	/* String/char have no endianness */
	endianness = NC_ENDIAN_NATIVE;
    } else { /* Numeric cases */
	switch(typelen) {
        case 1:
	    switch (tchar) {
  	    case 'i': nctype = NC_BYTE; break;
   	    case 'u': nctype = NC_UBYTE; break;
	    default: {stat = NC_ENCZARR; goto done;}
	    }
	    break;
        case 2:
  	    switch (tchar) {
	    case 'i': nctype = NC_SHORT; break;
	    case 'u': nctype = NC_USHORT; break;
	    default: {stat = NC_ENCZARR; goto done;}
	    }
	    break;
        case 4:
	    switch (tchar) {
	    case 'i': nctype = NC_INT; break;
	    case 'u': nctype = NC_UINT; break;
	    case 'f': nctype = NC_FLOAT; break;
	    default: {stat = NC_ENCZARR; goto done;}
	    }
	    break;
        case 8:
	    switch (tchar) {
	    case 'i': nctype = NC_INT64; break;
	    case 'u': nctype = NC_UINT64; break;
	    case 'f': nctype = NC_DOUBLE; break;
	    default: {stat = NC_ENCZARR; goto done;}
	    }
	    break;
        default: {stat = NC_ENCZARR; goto done;}
        }
    }

#if 0
    /* Convert NC_ENDIAN_NATIVE and NC_ENDIAN_NA */
    if(endianness == NC_ENDIAN_NATIVE)
        endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
#endif

    if(nctypep) *nctypep = nctype;
    if(typelenp) *typelenp = typelen;
    if(endianp) *endianp = endianness;

done:
    return stat;
}

/**
@internal Given an nc_type+purezarr+MAXSTRLEN, produce the corresponding Zarr v3 dtype string.
@param nctype     - [in] nc_type
@param purezarr   - [in] 1=>pure zarr, 0 => nczarr
@param strlen     - [in] max string length
@param namep      - [out] pointer to hold pointer to the dtype; user frees
@param tagp       - [out] pointer to hold pointer to the nczarr tag
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz3_nctype2dtype(nc_type nctype, int purezarr, int strlen, char** dnamep, const char** tagp)
{
    char dname[64];
    const char* dtype = NULL;
    const char* tag = NULL;

    if(nctype <= NC_NAT || nctype > N_NCZARR_TYPES) return NC_EINVAL;
    dtype = znamesv3[nctype].zarr;
    tag = znamesv3[nctype].type_alias;
    snprintf(dname,sizeof(dname),dtype,strlen*8);
    if(dnamep) *dnamep = strdup(dname);
    if(tagp) *tagp = tag;
    return NC_NOERR;		
}

/*
@internal Convert a Zarr v3 data_type spec to a corresponding nc_type.
@param dtype    - [in] dtype to convert
@param dalias   - [in] alias of dtype
@param nctypep  - [out] hold corresponding type
@param typelenp - [out] hold corresponding type size (for fixed length strings)
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

int
ncz3_dtype2nctype(const char* dtype , const char* dalias, nc_type* nctypep, size_t* typelenp)
{
    int stat = NC_NOERR;
    nc_type nctype = NC_NAT;
    size_t typelen = 0;

    if(nctypep) *nctypep = NC_NAT;
    if(typelenp) *typelenp = 0;

    /* handle netcdf type aliases */
    if(dalias != NULL) { 
	if(strcmp(dalias,"string")==0) {
	    nctype = NC_STRING;	    
	    if(dtype != NULL) {
	        if(1 != sscanf(dtype,"r%zu",&typelen)) {stat = NC_ENCZARR; goto done;}
		if((typelen % 8) != 0) {stat = NC_ENCZARR; goto done;}
		typelen = typelen / 8; /* convert bits to bytes */
	    }
	} else if(strcmp(dalias,"char")==0) {
	    nctype = NC_CHAR;
	    typelen = 1;
	} else if(strcmp(dalias,"json")==0) {
	    nctype = NC_JSON;
	    typelen = 0;
	} else
	    {dtype = dalias;}
    }
    if(nctype == NC_NAT) {
	int i, match;
	assert(dtype != NULL);
	/* short circuit handling of rn */
	if(1 == sscanf(dtype,"r%zu",&typelen)) {
	    nctype = NC_STRING;
   	    if((typelen % 8) != 0) {stat = NC_ENCZARR; goto done;}
	    typelen = typelen / 8; /* convert bits to bytes */
	    match = 1;
	} else for(match=0,i=0;i<N_NCZARR_TYPES;i++) {
	    if(znamesv3[i].zarr == NULL) continue;
	    if(strcmp(znamesv3[i].zarr,dtype)==0) {
                nctype = i;
                typelen = znamesv3[i].typelen;
		match = 1;
		break;
	    }
	}
	if(!match) {stat = NC_ENOTZARR; goto done;}
    }
    if(nctypep) *nctypep = nctype;
    if(typelenp) *typelenp = typelen;

done:
    return THROW(stat);
}

/* Infer the attribute's type based on its value(s).*/
int
NCZ_inferattrtype(const NCjson* values, nc_type typehint, nc_type* typeidp)
{
    int stat = NC_NOERR;
    nc_type typeid;
    unsigned long long u64;
    long long i64;
    int negative = 0;
    int singleton = 0;
    const NCjson* value = NULL;

    if(NCJsort(values) == NCJ_ARRAY && NCJarraylength(values) == 0)
        {typeid = NC_NAT; goto done;} /* Empty array is illegal */

    if(NCJsort(values) == NCJ_NULL)
        {typeid = NC_NAT; goto done;} /* NULL is also illegal */

    if(typehint == NC_JSON)
        {typeid = NC_JSON; goto done;}

    if(NCZ_iscomplexjson(values,typehint))
        {typeid = NC_JSON; goto done;}

    assert(NCJisatomic(values) || (NCJsort(values) == NCJ_ARRAY /*&& all i: NCJisatomic(NCJith(values)) == NCJ_ARRAY*/));

    /* Get the first element */
    if(NCJsort(values) == NCJ_ARRAY) {
	value = NCJith(values,0);
    } else if(NCJisatomic(values)) {
        value = values; /*singleton*/
	singleton = 1;
    }

    /* Look at the first element */
    switch (NCJsort(value)) {
    case NCJ_NULL:
    case NCJ_UNDEF:
	stat = NC_EINVAL;
	goto done;
    case NCJ_ARRAY:
    case NCJ_DICT:
    	typeid = NC_JSON;
	goto done;
    default: /* atomic type */
	break;
    }

    switch (NCJsort(value)) {
    case NCJ_INT:
        if(NCJstring(value) != NULL) negative = (NCJstring(value)[0] == '-');
	if(negative) {
	    sscanf(NCJstring(value),"%lld",&i64);
	    u64 = (unsigned long long)i64;
	} else
	    sscanf(NCJstring(value),"%llu",&u64);
	typeid = NCZ_inferinttype(u64,negative);
	break;
    case NCJ_DOUBLE:
	typeid = NC_DOUBLE;
	break;
    case NCJ_BOOLEAN:
	typeid = NC_UBYTE;
	break;
    case NCJ_STRING: /* requires special handling as an array of characters */
	typeid = NC_STRING;
	break;
    default:
	stat = NC_ENCZARR;
	goto done;
    }

    /* Infer NC_CHAR vs NC_STRING */
    if(typeid == NC_STRING) {
	if(singleton && NCJsort(value) == NCJ_STRING)
	    typeid = NC_CHAR;
	else if(NCJsort(values) == NCJ_ARRAY) {
	    int ischar1;
	    size_t i;
	    for(ischar1=1,i=0;i<NCJarraylength(values);i++) {
		NCjson* jelem = NCJith(values,i);
		if(NCJsort(jelem) != NCJ_STRING || strlen(NCJstring(jelem)) != 1) {ischar1 = 0; break;}
	    }
	    if(ischar1) typeid = NC_CHAR;
	}
    }

done:
    if(typeidp) *typeidp = typeid;
    return stat;
}

/* Infer the int type from the value;
   minimum type will be int.
*/
int
NCZ_inferinttype(unsigned long long u64, int negative)
{
    long long i64 = (long long)u64; /* keep bit pattern */
    if(!negative && u64 >= NC_MAX_INT64) return NC_UINT64;
    if(i64 < 0) {
	if(i64 >= NC_MIN_INT) return NC_INT;
	return NC_INT64;
    }
    if(i64 <= NC_MAX_INT) return NC_INT;
    if(i64 <= NC_MAX_UINT) return NC_UINT;
    return NC_INT64;
}
 
/**
@internal Similar to NCZ_grppath, but using group ids.
@param gid - [in] group id
@param pathp - [out] full path
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_grpname_full(int gid, char** pathp)
{
    int stat = NC_NOERR;
    size_t len;
    char* path = NULL;

    if((stat = nc_inq_grpname_full(gid,&len,NULL))) return stat;
    if((path=malloc(len+1)) == NULL) return NC_ENOMEM;    
    if((stat = nc_inq_grpname_full(gid,&len,path))) return stat;
    path[len] = '\0'; /* ensure null terminated */
    if(pathp) {*pathp = path; path = NULL;}
    return stat;
}

/**
@internal Parse a commified string list
@param s [in] string to parse
@param list - [in/out] storage for the parsed list
@return NC_NOERR
@author Dennis Heimbigner
*/
int
NCZ_comma_parse(const char* s, NClist* list)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* endp = NULL;

    if(s == NULL || *s == '\0') goto done;

    /* Split s at the commas or EOL */
    p = s;
    for(;;) {
	char* s;
	ptrdiff_t slen;
	endp = strchr(p,',');
	if(endp == NULL) endp = p + strlen(p);
	slen = (endp - p);
	if((s = malloc((size_t)slen+1)) == NULL) {stat = NC_ENOMEM; goto done;}
	memcpy(s,p,(size_t)slen);
	s[slen] = '\0';
	if(nclistmatch(list,s,0)) {
	    nullfree(s); /* duplicate */
	} else {
	    nclistpush(list,s);
	}
	if(*endp == '\0') break;
	p = endp+1;
    }

done:
    return stat;
}

/**************************************************/
/* Endianness support */
/* signature: void swapinline16(void* ip) */
#define swapinline16(ip) \
{ \
    union {char b[2]; unsigned short i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[1]; \
    u.b[1] = src[0]; \
    *((unsigned short*)ip) = u.i; \
}

/* signature: void swapinline32(void* ip) */
#define swapinline32(ip) \
{ \
    union {char b[4]; unsigned int i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[3]; \
    u.b[1] = src[2]; \
    u.b[2] = src[1]; \
    u.b[3] = src[0]; \
    *((unsigned int*)ip) = u.i; \
}

/* signature: void swapinline64(void* ip) */
#define swapinline64(ip) \
{ \
    union {char b[8]; unsigned long long i;} u; \
    char* src = (char*)(ip); \
    u.b[0] = src[7]; \
    u.b[1] = src[6]; \
    u.b[2] = src[5]; \
    u.b[3] = src[4]; \
    u.b[4] = src[3]; \
    u.b[5] = src[2]; \
    u.b[6] = src[1]; \
    u.b[7] = src[0]; \
    *((unsigned long long*)ip) = u.i; \
}

int
NCZ_swapatomicdata(size_t datalen, void* data, int typesize)
{
    int stat = NC_NOERR;
    int i;

    assert(datalen % typesize == 0);

    if(typesize == 1) goto done;

    /*(typesize > 1)*/
    for(i=0;i<datalen;) {
	char* p = ((char*)data) + i;
        switch (typesize) {
        case 2: swapinline16(p); break;
        case 4: swapinline32(p); break;
        case 8: swapinline64(p); break;
        default: break;
	}
	i += typesize;
    }
done:
    return THROW(stat);
}

char**
NCZ_clonestringvec(size_t len, const char** vec)
{
    char** clone = NULL;
    size_t i;
    if(vec == NULL) return NULL;
    if(len == 0) { /* Figure out size as envv vector */
        const char** p;
        for(p=vec;*p;p++) len++;
    }
    clone = malloc(sizeof(char*) * (1+len));
    if(clone == NULL) return NULL;
    for(i=0;i<len;i++) {
	char* s = strdup(vec[i]);
	if(s == NULL) return NULL;
	clone[i] = s;
    }
    clone[len] = NULL;
    return clone;
}

void
NCZ_freestringvec(size_t len, char** vec)
{
    size_t i;
    if(vec == NULL) return;
    if(len == 0) { /* Figure out size as envv vector */
        char** p;
        for(p=vec;*p;p++) len++;
    }
    for(i=0;i<len;i++) {
	nullfree(vec[i]);
    }
    nullfree(vec);
}

int
NCZ_ischunkname(const char* name,char dimsep)
{
    int stat = NC_NOERR;
    const char* p;
    if(strchr("0123456789",name[0])== NULL)
        stat = NC_ENCZARR;
    else for(p=name;*p;p++) {
        if(*p != dimsep && strchr("0123456789",*p) == NULL) /* approximate */
	    {stat = NC_ENCZARR; break;}
    }
    return stat;
}

char*
NCZ_chunkpath(struct ChunkKey key)
{
    size_t plen = nulllen(key.varkey)+1+nulllen(key.chunkkey);
    char* path = (char*)malloc(plen+1);
    
    if(path == NULL) return NULL;
    path[0] = '\0';
    strlcat(path,key.varkey,plen+1);
    strlcat(path,"/",plen+1);
    strlcat(path,key.chunkkey,plen+1);
    return path;    
}

int
NCZ_reclaim_fill_value(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    if(var->fill_value) {
	int tid = var->type_info->hdr.id;
	stat = NC_reclaim_data_all(var->container->nc4_info->controller,tid,var->fill_value,1);
	var->fill_value = NULL;
    }
    /* Reclaim any existing fill_chunk */
    if(!stat) stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache);
    return stat;
}

int
NCZ_copy_fill_value(NC_VAR_INFO_T* var, void**  dstp)
{
    int stat = NC_NOERR;
    int tid = var->type_info->hdr.id;
    void* dst = NULL;

    if(var->fill_value) {
	if((stat = NC_copy_data_all(var->container->nc4_info->controller,tid,var->fill_value,1,&dst))) goto done;
    }
    if(dstp) {*dstp = dst; dst = NULL;}
done:
    if(dst) (void)NC_reclaim_data_all(var->container->nc4_info->controller,tid,dst,1);
    return stat;
}


/* Get max str len for a variable or grp */
/* Has side effect of setting values in the
   internal data structures */
int
NCZ_get_maxstrlen(NC_OBJ* obj)
{
    int maxstrlen = 0;
    assert(obj->sort == NCGRP || obj->sort == NCVAR);
    if(obj->sort == NCGRP) {
        NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)obj;
	NC_FILE_INFO_T* file = grp->nc4_info;
	NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
	if(zfile->default_maxstrlen == 0)
	    zfile->default_maxstrlen = NCZ_MAXSTR_DEFAULT;
	maxstrlen = zfile->default_maxstrlen;
    } else { /*(obj->sort == NCVAR)*/
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)obj;
	NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
        if(zvar->maxstrlen == 0)
	    zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var->container);
	maxstrlen = zvar->maxstrlen;
    }
    return maxstrlen;
}

int
NCZ_fixed2char(const void* fixed, char** charp, size_t count, int maxstrlen)
{
    size_t i;
    unsigned char* sp = NULL;
    const unsigned char* p = fixed;
    memset((void*)charp,0,sizeof(char*)*count);
    for(i=0;i<count;i++,p+=maxstrlen) {
	if(p[0] == '\0') {
	    sp = NULL;
	} else {
	    if((sp = (unsigned char*)malloc((size_t)maxstrlen+1))==NULL) /* ensure null terminated */
	        return NC_ENOMEM; 
	    memcpy(sp,p,(size_t)maxstrlen);
	    sp[maxstrlen] = '\0';
	}
	charp[i] = (char*)sp;
	sp = NULL;
    }
    return NC_NOERR;
}

int
NCZ_char2fixed(const char** charp, void* fixed, size_t count, int maxstrlen)
{
    size_t i;
    unsigned char* p = fixed;
    memset(fixed,0,maxstrlen*count); /* clear target */
    for(i=0;i<count;i++,p+=maxstrlen) {
	size_t len;
	if(charp[i] != NULL) {
	    len = strlen(charp[i]);
	    if(len > maxstrlen) len = maxstrlen;
	    memcpy(p,charp[i],len);
	} else {
	    memset(p,'\0',maxstrlen);
	}
    }
    return NC_NOERR;
}

/*
Wrap NC_copy_data, but take string value into account when overwriting
*/
int
NCZ_copy_data(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const void* memory, size_t count, int reading, void* copy)
{
    int stat = NC_NOERR;    
    NC_TYPE_INFO_T* xtype = var->type_info;
    if(xtype->hdr.id == NC_STRING && !reading) {
	size_t i;
	char** scopy = (char**)copy;
	/* Reclaim any string fill values in copy */
	for(i=0;i<count;i++) {
	    nullfree(scopy[i]);
	    scopy[i] = NULL;
	}
    }
    stat = NC_copy_data(file->controller,xtype->hdr.id,memory,count,copy);
    return stat;
}

#if 0
/* Recursive helper */
static int
checksimplejson(NCjson* json, int depth)
{
    int i;

    switch (NCJsort(json)) {
    case NCJ_ARRAY:
	if(depth > 0) return 0;  /* e.g. [...,[...],...]  or [...,{...},...] */
	for(i=0;i < NCJarraylength(json);i++) {
	    NCjson* j = NCJith(json,i);
	    if(!checksimplejson(j,depth+1)) return 0;
        }
	break;
    case NCJ_DICT:
    case NCJ_NULL:
    case NCJ_UNDEF:
	return 0;
    default: break;
    }
    return 1;
}
#endif

/* Return 1 if the attribute will be stored as a complex JSON valued attribute; return 0 otherwise */
int
NCZ_iscomplexjson(const NCjson* json, nc_type typehint)
{
    int i, stat = 0;

    switch (NCJsort(json)) {
    case NCJ_ARRAY:
	/* If the typehint is NC_JSON, then always treat it as complex */
	if(typehint == NC_JSON) {stat = 1; goto done;}
	/* Otherwise see if it is a simple vector of atomic values */
	for(i=0;i < NCJarraylength(json);i++) {
	    NCjson* j = NCJith(json,i);
	    if(!NCJisatomic(j)) {stat = 1; goto done;}
        }
	break;
    case NCJ_DICT:
    case NCJ_NULL:
    case NCJ_UNDEF:
	stat = 1; goto done;
    default: break;
    }
done:
    return stat;
}

/* Return 1 if the attribute value as a string should be stored as complex json
Assumes attribute type is NC_CHAR.
See zutil.c documentation.
@param text of the attribute as a string
@param jsonp return the parsed json here (if parseable)
@return 1 if is complex json
*/
int
NCZ_iscomplexjsontext(size_t textlen, const char* text, NCjson** jsonp)
{
    NCjson* json = NULL;
    const char* p;
    int iscomplex, instring;
    size_t i;

    if(jsonp) *jsonp = NULL;
    if(text == NULL || textlen < 2) return 0;

    instring = 0;
    iscomplex = 0;
    for(i=0,p=text;i<textlen;i++,p++) {
	switch (*p) {
	case '\\': p++; break;
	case '"': instring = (instring?0:1); break;
	case '[': case '{': case ']': case '}':
	    iscomplex=1;
	    goto loopexit;
	    break;
	default: break;
	}
    }
loopexit:
    if(!iscomplex) return 0;
    /* Final test: must be parseable */
    if(NCJparsen(textlen,text,0,&json) < 0) /* not JSON parseable */
        return 0;
    if(json == NULL) return 0;
    if(jsonp) {*jsonp = json; json = NULL;}
    NCJreclaim(json);
    return 1;
}

/* Caller must free return value */
int
NCZ_makeFQN(NC_GRP_INFO_T* parent, NC_OBJ* object, NCbytes* fqn)
{
    int i, stat = NC_NOERR;
    NClist* segments = nclistnew();
    NC_GRP_INFO_T* grp = NULL;
    char* escaped = NULL;

    /* Add in the object name */
    if((escaped = NCZ_backslashescape(object->name))==NULL) goto done;
    nclistpush(segments,escaped);
    escaped = NULL;

    /* Collect the group prefix segments (escaped) */
    for(grp=parent;grp->parent!=NULL;grp=grp->parent) {
	/* Add in the group name */
	if((escaped = NCZ_backslashescape(grp->hdr.name))==NULL) goto done;
        nclistpush(segments,escaped);
	escaped = NULL;
    }
    
    /* Create the the fqn */
    for(i=(nclistlength(segments)-1);i>=0;i--) {
	ncbytescat(fqn,"/");
	ncbytescat(fqn,nclistget(segments,i));
    }

done:
    nclistfreeall(segments);
    nullfree(escaped);
    return THROW(stat);
}

/* Find an object matching the given name and of given sort */
int
NCZ_locateFQN(NC_GRP_INFO_T* parent, const char* fqn, NC_SORT sort, NC_OBJ** objectp)
{
    int i,ret = NC_NOERR;
    NC_GRP_INFO_T* grp = NULL;
    NC_OBJ* object = NULL;
    NClist* segments = nclistnew();
    size_t count = 0;

    assert(fqn != NULL && fqn[0] == '/');
    /* Step 1: Break fqn into segments at occurrences of '/' */
    if((ret = splitfqn(fqn,segments))) goto done;
    count = nclistlength(segments);

    /* walk to convert to groups + 1 left over for the final object*/
    grp = parent;
    for(i=0;i<count-1;i++){
	const char* segment = (const char*)nclistget(segments,i);
	NC_OBJ* object = NULL;
	/* Walk the group prefixes */
	object = ncindexlookup(grp->children,segment);
	if(object == NULL || object->sort != NCGRP) {ret = NC_ENOOBJECT; goto done;}
	grp = (NC_GRP_INFO_T*)object; object = NULL;
    }
    /* Find an object to match the sort and last segment */
    do {
	const char* segment = (const char*)nclistget(segments,count-1); /* last segment */
        object = ncindexlookup(grp->children,segment);
        if(object != NULL && (sort == NCNAT || sort == NCGRP)) break; /* not this */
        object = ncindexlookup(grp->dim,segment);
        if(object != NULL && (sort == NCNAT || sort == NCDIM)) break; /* not this */
        object = ncindexlookup(grp->vars,segment);
        if(object != NULL && (sort == NCNAT || sort == NCVAR)) break; /* not this */
	object = ncindexlookup(grp->type,segment);
        if(object != NULL && (sort == NCNAT || sort == NCTYP)) break; /* not this */
	object = ncindexlookup(grp->att,segment);
        if(object != NULL && (sort == NCNAT || sort == NCATT)) break; /* not this */
	object = NULL; /* not found */
    } while(0);
    if(object == NULL) {ret = NC_ENOOBJECT; goto done;}
    if(objectp) *objectp = object;
done:
    nclistfreeall(segments);
    return THROW(ret);
}

static int
splitfqn(const char* fqn0, NClist* segments)
{
    int i,stat = NC_NOERR;
    char* fqn = NULL;
    char* p = NULL;
    char* start = NULL;
    int count = 0;

    assert(fqn0 != NULL && fqn0[0] == '/');
    fqn = strdup(fqn0);
    start = fqn+1; /* leave off the leading '/' */
    if(strlen(start) > 0) count=1; else count = 0;
    /* Break fqn into pieces at occurrences of '/' */
    for(p=start;*p;) {
	switch(*p) {
	case '\\':
	    p+=2;
	    break;
	case '/': /*capture the piece name */
	    *p++ = '\0';
	    start = p; /* mark start of the next part */
	    count++;
	    break;
	default: /* ordinary char */
	    p++;
	    break;
	}
    }
    /* collect segments */
    p = fqn+1;
    for(i=0;i<count;i++){
	char* descaped = NCZ_deescape(p);
	nclistpush(segments,descaped);
    }
    nullfree(fqn);
    return stat;
}

char*
NCZ_backslashescape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;

    len = strlen(s);
    escaped = (char*)malloc(1+(2*len)); /* max is everychar is escaped */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
        char c = *p;
        switch (c) {
	case '\\':
	case '/':
	case '.':
	case '@':
	    *q++ = '\\'; *q++ = '\\';
	    break;
	default: *q++ = c; break;
        }
    }
    *q = '\0';
    return escaped;
}

char*
NCZ_deescape(const char* esc)
{
    size_t len;
    char* s;
    const char* p;
    char* q;

    if(esc == NULL) return NULL;
    len = strlen(esc);
    s = (char*)malloc(len+1);
    if(s == NULL) return NULL;
    for(p=esc,q=s;*p;) {
	switch (*p) {
	case '\\':
	     p++;
	     /* fall thru */
	default: *q++ = *p++; break;
	}
    }
    *q = '\0';
    return s;
}

/* Define a static qsort comparator for strings for use with qsort.
   It sorts by length and then content */
static int
cmp_strings(const void* a1, const void* a2)
{
    const char** s1 = (const char**)a1;
    const char** s2 = (const char**)a2;
    size_t slen1 = strlen(*s1);
    size_t slen2 = strlen(*s2);
    if(slen1 != slen2) return (slen1 - slen2);
    return strcmp(*s1,*s2);
}

int
NCZ_sortstringlist(void* vec, size_t count)
{
    if(vec != NULL && count > 0) {
        qsort(vec, count, sizeof(void*), cmp_strings);
    }
    return NC_NOERR;
}

#if 0
/* Define a static qsort comparator for JSON dict (key,value) pairs */
static int
cmp_ncjson(const void* a1, const void* a2)
{
    const NCjson** j1 = (const NCjson**)a1;
    const NCjson** j2 = (const NCjson**)a2;
    return strcmp(NCJstring(*j1),NCJstring(*j2));
}

int
NCZ_sortpairlist(void* vec, size_t count)
{
    if(vec != NULL && count > 0) {
        qsort(vec, count, 2*sizeof(void*), cmp_ncjson);
    }
    return NC_NOERR;
}
#endif

void
NCZ_freeAttrInfoVec(struct NCZ_AttrInfo* ainfo)
{
    if(ainfo == NULL) return;
    free(ainfo);
}
