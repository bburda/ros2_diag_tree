Logs
====

.. req:: GET /{entity}/logs
   :id: REQ_INTEROP_061
   :status: open
   :tags: Logs

   The endpoint shall provide an overview of log sources and log configuration for the addressed entity.

.. req:: GET /{entity}/logs/entries
   :id: REQ_INTEROP_062
   :status: open
   :tags: Logs

   The endpoint shall return log entries for the addressed entity, optionally in a paged manner.

.. req:: GET /{entity}/logs/config
   :id: REQ_INTEROP_063
   :status: open
   :tags: Logs

   The endpoint shall return the current logging configuration of the addressed entity.

.. req:: PUT /{entity}/logs/config
   :id: REQ_INTEROP_064
   :status: open
   :tags: Logs

   The endpoint shall update the logging configuration of the addressed entity.

.. req:: DELETE /{entity}/logs/config
   :id: REQ_INTEROP_065
   :status: open
   :tags: Logs

   The endpoint shall reset the logging configuration of the addressed entity to default settings.

