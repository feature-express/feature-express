Here's a detailed implementation plan to create a column-oriented event storage based on the points you mentioned and the `Event` data structure:

1. **Define Table Schema per Event Type**: Each event type might have different attributes. It would be beneficial to treat each event type as a separate table with its column schema. This will ensure that you do not have sparse columns.

2. **Time-based Chunking**: Partition the data into time-based chunks, such as days or other configurable time periods. Each chunk can be thought of as a separate table segment.

3. **Chunk Index**: Create an index that keeps track of the chunks. This could be a simple mapping from time periods to chunk metadata (like file paths if stored on disk).

4. **BTree Index for Timestamps within Chunks**: For each chunk, create a BTree index for the `event_time` field. This will facilitate efficient range queries within each chunk.

5. **Data Ingestion**: As events are ingested, they should be appended to the appropriate chunk based on their `event_time`. Initially, do not compress/encode the data. This allows for fast writes and enables analysis of the data for later optimization.

6. **Compression Analysis and Encoding**: After a certain threshold (e.g., end of the day or when the chunk reaches a certain size), analyze the data in the chunk to determine the optimal compression/encoding strategy for each column. Implement the selected strategies and compress the data. Store metadata about the chosen strategies.

7. **Flatten Nested Values**: For `Value` fields that are nested (e.g., `Map` or `Vec`), flatten them into separate columns. This might involve creating additional tables or columns to represent these structures.

8. **Create Secondary Indices**: Besides the BTree index for `event_time`, evaluate if additional indices are needed for frequently queried columns. For example, creating indices on `entities` might be useful if queries often filter or join on these values.

9. **Compaction and Maintenance**: Over time, as data is inserted and updated, the storage might become fragmented or less optimized. Implement maintenance routines that can compact and optimize the storage. This might involve merging smaller chunks, re-encoding data, or rebuilding indices.

10. **Query Engine**: Implement a query engine that can efficiently execute queries across multiple chunks. The query engine should be able to leverage the indices, perform partition pruning based on time ranges, and decode/compress data as needed.

11. **Concurrency Control and ACID Properties**: If the storage system needs to support concurrent writes and reads, implement concurrency control mechanisms like locking or multi-version concurrency control (MVCC). Also, consider how to ensure Atomicity, Consistency, Isolation, and Durability (ACID) properties if required by your use case.

12. **Testing and Optimization**: Create extensive test suites to ensure the correctness of the data stored and the queries. Monitor the performance and make optimizations as needed.

13. **Documentation and User Guide**: Document the architecture, data structures, APIs, and query capabilities of the storage system. Create user guides and examples to help users effectively use the system.

14. **Monitoring and Logging**: Implement monitoring and logging to track the performance, resource usage, and errors in the storage system. This is crucial for diagnosing issues and optimizing performance.

This plan outlines the steps and considerations for creating a column-oriented event storage system. Remember that building a storage system is complex, and careful design and extensive testing are critical to ensure its reliability and performance.