BulkData
========

.. req:: GET /{entity}/bulk-data
   :id: REQ_INTEROP_071
   :status: open
   :tags: BulkData

   The endpoint shall provide an overview of bulk-data categories supported by the addressed entity.

.. req:: GET /{entity}/bulk-data/{category}
   :id: REQ_INTEROP_072
   :status: open
   :tags: BulkData

   The endpoint shall list all bulk-data items available in the addressed bulk-data category on the entity.

.. req:: GET /{entity}/bulk-data/{category}/{bulk-data-id}
   :id: REQ_INTEROP_073
   :status: open
   :tags: BulkData

   The endpoint shall return the content of the addressed bulk-data item or its access information (e.g. download location).

.. req:: POST /{entity}/bulk-data/{category}
   :id: REQ_INTEROP_074
   :status: open
   :tags: BulkData

   The endpoint shall upload new bulk data in the addressed category and create a corresponding bulk-data resource on the entity.

.. req:: DELETE /{entity}/bulk-data/{category}/{bulk-data-id}
   :id: REQ_INTEROP_075
   :status: open
   :tags: BulkData

   The endpoint shall delete the addressed bulk-data item from the entity, if permitted.

