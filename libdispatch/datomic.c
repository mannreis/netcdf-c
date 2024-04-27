/*
  Copyright 2018 University Corporation for Atmospheric
  Research/Unidata. See \ref copyright file for more info.
*/
/**
 * @file
 * @internal Functions for Atomic Types
 *
 * @author Dennis Heimbigner
 * Note: this file should grow to consolidate atomic type functions.
*/

#include "ncdispatch.h"

/* The sizes of types may vary from platform to platform, but within
 * netCDF files, type sizes are fixed. */
#define NC_CHAR_LEN sizeof(char)      /**< @internal Size of char. */
#define NC_STRING_LEN sizeof(char *)  /**< @internal Size of char *. */
#define NC_BYTE_LEN 1     /**< @internal Size of byte. */
#define NC_SHORT_LEN 2    /**< @internal Size of short. */
#define NC_INT_LEN 4      /**< @internal Size of int. */
#define NC_FLOAT_LEN 4    /**< @internal Size of float. */
#define NC_DOUBLE_LEN 8   /**< @internal Size of double. */
#define NC_INT64_LEN 8    /**< @internal Size of int64. */

/** @internal Names of atomic types. */
const char* nc4_atomic_name[NUM_ATOMIC_TYPES] = {"none", "byte", "char",
                                           "short", "int", "float",
                                           "double", "ubyte",
                                           "ushort", "uint",
                                           "int64", "uint64", "string"};
static const size_t nc4_atomic_size[NUM_ATOMIC_TYPES] = {0, NC_BYTE_LEN, NC_CHAR_LEN, NC_SHORT_LEN,
                                                      NC_INT_LEN, NC_FLOAT_LEN, NC_DOUBLE_LEN,
                                                      NC_BYTE_LEN, NC_SHORT_LEN, NC_INT_LEN, NC_INT64_LEN,
                                                      NC_INT64_LEN, NC_STRING_LEN};

/** \defgroup atomic_types Atomic Types (not including NC_STRING) */
/** \{

\ingroup atomic_types
*/

/**
 * @internal Get the name and size of an atomic type. For strings, 1 is
 * returned.
 *
 * @param typeid1 Type ID.
 * @param name Gets the name of the type.
 * @param size Gets the size of one element of the type in bytes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Dennis Heimbigner
 */
int
NC4_inq_atomic_type(nc_type typeid1, char *name, size_t *size)
{
    if (typeid1 >= NUM_ATOMIC_TYPES)
	return NC_EBADTYPE;
    if (name)
            strcpy(name, nc4_atomic_name[typeid1]);
    if (size)
            *size = nc4_atomic_size[typeid1];
    return NC_NOERR;
}

/**
 * @internal Get the id and size of an atomic type by name.
 *
 * @param name [in] the name of the type.
 * @param idp [out] the type index of the type.
 * @param sizep [out] the size of one element of the type in bytes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Dennis Heimbigner
 */
int
NC4_lookup_atomic_type(const char *name, nc_type* idp, size_t *sizep)
{
    int i;

    if (name == NULL || strlen(name) == 0)
	return NC_EBADTYPE;
    for(i=0;i<NUM_ATOMIC_TYPES;i++) {
	if(strcasecmp(name,nc4_atomic_name[i])==0) {	
	    if(idp) *idp = i;
            if(sizep) *sizep = nc4_atomic_size[i];
	    return NC_NOERR;
        }
    }
    return NC_EBADTYPE;
}

/**
 * @internal Get the id of an atomic type from the name.
 *
 * @param ncid File and group ID.
 * @param name Name of type
 * @param typeidp Pointer that will get the type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
int
NC4_inq_atomic_typeid(int ncid, const char *name, nc_type *typeidp)
{
    int i;

    NC_UNUSED(ncid);

    /* Handle atomic types. */
    for (i = 0; i < NUM_ATOMIC_TYPES; i++) {
        if (!strcmp(name, nc4_atomic_name[i]))
        {
            if (typeidp)
                *typeidp = i;
	    return NC_NOERR;
        }
    }
    return NC_EBADTYPE;
}

/**
 * @internal Get the class of a type
 *
 * @param xtype NetCDF type ID.
 * @param type_class Pointer that gets class of type, NC_INT,
 * NC_FLOAT, NC_CHAR, or NC_STRING, NC_ENUM, NC_VLEN, NC_COMPOUND, or
 * NC_OPAQUE.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_get_atomic_typeclass(nc_type xtype, int *type_class)
{
    assert(type_class);
    switch (xtype) {
        case NC_BYTE:
        case NC_UBYTE:
        case NC_SHORT:
        case NC_USHORT:
        case NC_INT:
        case NC_UINT:
        case NC_INT64:
        case NC_UINT64:
            /* NC_INT is class used for all integral types */
            *type_class = NC_INT;
            break;
        case NC_FLOAT:
        case NC_DOUBLE:
            /* NC_FLOAT is class used for all floating-point types */
            *type_class = NC_FLOAT;
            break;
        case NC_CHAR:
            *type_class = NC_CHAR;
            break;
        case NC_STRING:
            *type_class = NC_STRING;
            break;
        default:
	   return NC_EBADTYPE;
        }
    return NC_NOERR;
}

/** \} */
