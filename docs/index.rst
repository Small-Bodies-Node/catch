.. Catch documentation master file, created by
   sphinx-quickstart on Mon Apr 22 09:11:54 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Catch's documentation!
=================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:


Caching system
==============

Reseting the cache
------------------

Cached results may become stale and require removal to avoid returning bad data to users.  This is more common in development, when CATCH's survey metadata is more volatile.  For example, a CATCH development instance was used to test review versions of the LONEOS archive, but for completeness only the final data set should be indexed with CATCH.  There are two approaches that can be used, both require read/write access to the database with Postgres:

1. Delete previous queries from the `catch_query` and `found` tables.  To only remove a portion of the data, e.g., just the LONEOS queries:

   .. code-block:: sql

      BEGIN;
         DELETE FROM found USING catch_query WHERE found.query_id = catch_query.query_id AND catch_query.source = 'loneos';
         DELETE FROM catch_query WHERE source = 'loneos';
      END;

   Alternatively, remove all rows from both tables:

   .. code-block:: sql

      BEGIN;
         DELETE FROM found WHERE TRUE;
         DELETE FROM catch_query WHERE TRUE;
      END;
      
2. Mark previous queries as "expired".  CATCH uses string matching to find valid queries using the "status" column.  To prevent queries marked "finished" from being used, append another string, such as ", expired":

   .. code-block:: sql

      UPDATE catch_query SET status = status || ', expired' WHERE source = 'loneos' AND status NOT LIKE '%expired';

The choice depends on the importance of the previous queries.  Deleting the data removes the usage history, including performance metrics.  But during a major version update, all previous queries may be irrelevant, and should be removed.  One should make a copy of the data to guard against information loss.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
