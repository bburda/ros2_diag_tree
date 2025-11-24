Faults
======

.. req:: GET /{entity}/faults
   :id: REQ_INTEROP_012
   :status: open
   :tags: Faults

   The endpoint shall return the list of diagnostic fault entries stored for the addressed entity, possibly including active and stored faults.

.. req:: GET /{entity}/faults/{code}
   :id: REQ_INTEROP_013
   :status: open
   :tags: Faults

   The endpoint shall return detailed information for the addressed diagnostic fault code.

.. req:: DELETE /{entity}/faults
   :id: REQ_INTEROP_014
   :status: open
   :tags: Faults

   The endpoint shall clear all diagnostic fault entries stored for the addressed entity, if permitted.

.. req:: DELETE /{entity}/faults/{code}
   :id: REQ_INTEROP_015
   :status: open
   :tags: Faults

   The endpoint shall clear the addressed diagnostic fault code for the entity, if permitted.

