Operations
==========

.. req:: GET /{entity}/operations
   :id: REQ_INTEROP_033
   :status: open
   :tags: Operations

   The endpoint shall list all supported operations that can be executed on the addressed entity.

.. req:: GET /{entity}/operations/{op-id}
   :id: REQ_INTEROP_034
   :status: open
   :tags: Operations

   The endpoint shall return the definition and metadata of the addressed operation.

.. req:: POST /{entity}/operations/{op-id}/executions
   :id: REQ_INTEROP_035
   :status: open
   :tags: Operations

   The endpoint shall start a new execution of the addressed operation on the entity.

.. req:: GET /{entity}/operations/{op-id}/executions
   :id: REQ_INTEROP_036
   :status: open
   :tags: Operations

   The endpoint shall list active and past executions of the addressed operation.

.. req:: GET /{entity}/operations/{op-id}/executions/{exec-id}
   :id: REQ_INTEROP_037
   :status: open
   :tags: Operations

   The endpoint shall return the current status and any result details of the addressed operation execution.

.. req:: PUT /{entity}/operations/{op-id}/executions/{exec-id}
   :id: REQ_INTEROP_038
   :status: open
   :tags: Operations

   The endpoint shall control the addressed operation execution (e.g. execute, freeze, reset or other supported capabilities) and may update execution parameters, if supported.

.. req:: DELETE /{entity}/operations/{op-id}/executions/{exec-id}
   :id: REQ_INTEROP_039
   :status: open
   :tags: Operations

   The endpoint shall terminate the addressed operation execution (if still running) and remove its execution resource, if cancellation is supported.

