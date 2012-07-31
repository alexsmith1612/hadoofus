cdef extern from "sasl/sasl.h":
    cdef struct sasl_callback_t:
        pass
    cdef int SASL_OK "SASL_OK"
    cdef int sasl_client_init(sasl_callback_t*)
    cdef void sasl_done()
