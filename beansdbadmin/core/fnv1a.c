#include <Python.h>
#include <stdint.h>

#define FNV_32_PRIME 0x01000193U
#define FNV_32_INIT 0x811c9dc5U


static int32_t
hash_fnv1a(const char *key, int key_len) {
  int32_t h = FNV_32_INIT;
  int i;

  for (i=0; i<key_len; i++) {
	  h ^= (int32_t)key[i];
	  h *= FNV_32_PRIME;
  }

  return h;
}

static uint32_t
hash_fnv1a_bugfree(const unsigned char *key, int key_len) {
	uint32_t h = FNV_32_INIT;
	int i;

	for (i=0; i<key_len; i++) {
		h ^= (uint32_t)key[i];
		h *= FNV_32_PRIME;
	}

	return h;
}


static uint32_t
hash_fnv1a_beansdb(const char *key, int key_len) {
	uint32_t h = FNV_32_INIT;
	int i;

	for (i=0; i<key_len; i++) {
		h ^= (uint32_t)key[i];
		h *= FNV_32_PRIME;
	}

	return h;
}


static PyObject * get_hash(PyObject *self,PyObject *args) {
    char * guid;
    int len;
    if(!PyArg_ParseTuple(args,"s#",&guid,&len)) {
        return NULL;
    }
    int32_t h =  hash_fnv1a(guid,strlen(guid));
    /* return Py_BuildValue("l",h); */
#if PY_MAJOR_VERSION >= 3
    return PyLong_FromLong(h);
#else
    return PyInt_FromLong(h);
#endif
}

static PyObject * get_hash_bugfree(PyObject *self, PyObject *args) {
	char * guid;
	int len;

	if (!PyArg_ParseTuple(args, "s#", &guid, &len)) {
		return NULL;
	}

	uint32_t h = hash_fnv1a_bugfree(guid, len);
#if PY_MAJOR_VERSION >= 3
	return PyLong_FromLong((int32_t)h);
#else
	return PyInt_FromLong((int32_t)h);
#endif
}


static PyObject * get_hash_beansdb(PyObject *self, PyObject *args) {
	char * guid;
	int len;

	if (!PyArg_ParseTuple(args, "s#", &guid, &len)) {
		return NULL;
	}

	uint32_t h = hash_fnv1a_beansdb(guid, len);
#if PY_MAJOR_VERSION >= 3
	return PyLong_FromLong(h);
#else
	return PyInt_FromLong(h);
#endif
}


static PyMethodDef methods[] = {
    {"get_hash", (PyCFunction)get_hash, METH_VARARGS,
	    "fnv1a.get_hash() is buggy! Use fnv1a.get_hash_bugfree() instead!"},
    {"get_hash_bugfree", (PyCFunction)get_hash_bugfree, METH_VARARGS,
	    "get_hash_bugfree(string) -> int.\n\n get fnv1a 32bit hash value"},
    {"get_hash_beansdb", (PyCFunction)get_hash_beansdb, METH_VARARGS,
	    "fnv1a.get_hash_beansdb() is buggy! Use fnv1a.get_hash_bugfree() instead!"},
    {NULL,NULL,0,NULL}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "fnv1a",     /* m_name */
    "fnv1a hash algorithm extension module.",  /* m_doc */
    -1,                  /* m_size */
    methods,    /* m_methods */
    NULL,                /* m_reload */
    NULL,                /* m_traverse */
    NULL,                /* m_clear */
    NULL,                /* m_free */
};

PyMODINIT_FUNC
PyInit_fnv1a(void)
{
    return PyModule_Create(&moduledef);
}
#else
PyMODINIT_FUNC initfnv1a() {
    Py_InitModule3("fnv1a", methods, "fnv1a hash algorithm extension module.");
}
#endif

