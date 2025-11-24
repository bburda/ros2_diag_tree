Updates
=======

.. req:: GET /updates
   :id: REQ_INTEROP_082
   :status: open
   :tags: Updates

   The endpoint shall list software update packages known to the SOVD server.

.. req:: POST /updates
   :id: REQ_INTEROP_083
   :status: open
   :tags: Updates

   The endpoint shall register a new software update package at the SOVD server using the provided parameters.

.. req:: DELETE /updates/{id}
   :id: REQ_INTEROP_084
   :status: open
   :tags: Updates

   The endpoint shall delete the addressed software update package from the SOVD server, if permitted (e.g. if no update is currently in execution).

.. req:: GET /updates/{id}
   :id: REQ_INTEROP_085
   :status: open
   :tags: Updates

   The endpoint shall return the details of the addressed software update package.

