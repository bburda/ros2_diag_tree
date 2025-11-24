Coverage Report
===============

This page provides an overview of the requirement coverage by tests.

Statistics
----------

.. needpie:: Requirement Verification Status
   :labels: Verified, Missing Verification
   :colors: #4CAF50, #F44336
   :text_color: #000000

   len(verifies_back) > 0
   len(verifies_back) == 0

Missing Verification
--------------------

The following requirements are not yet linked to any test case.

.. needtable::
   :filter: type == 'req' and len(verifies_back) == 0
   :columns: id, title, tags
   :style: table

Traceability Matrix
-------------------

.. needtable::
   :filter: type == 'req'
   :columns: id, title, verifies_back
   :style: table
