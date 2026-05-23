package ai.streamforge.processor;

import ai.streamforge.processor.sink.IcebergSinkFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IcebergSinkFactoryTest {

    // ── REST catalog properties ──────────────────────────────────────────────

    @Test
    void restCatalog_setsUriFromRestUri() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "rest", "s3://warehouse/", "http://minio:9000",
                "key", "secret", "http://iceberg-rest:8181");
        assertEquals("http://iceberg-rest:8181", props.get("uri"));
    }

    @Test
    void restCatalog_setsS3FileIo() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "rest", "s3://warehouse/", "http://minio:9000",
                "key", "secret", "http://iceberg-rest:8181");
        assertEquals("org.apache.iceberg.aws.s3.S3FileIO", props.get("io-impl"));
    }

    @Test
    void restCatalog_setsS3Endpoint() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "rest", "s3://warehouse/", "http://minio:9000",
                "key", "secret", "http://iceberg-rest:8181");
        assertEquals("http://minio:9000", props.get("s3.endpoint"));
        assertEquals("key",              props.get("s3.access-key-id"));
        assertEquals("secret",           props.get("s3.secret-access-key"));
        assertEquals("true",             props.get("s3.path-style-access"));
    }

    @Test
    void restCatalog_noS3Endpoint_skipsS3Props() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "rest", "s3://warehouse/", "", "", "", "http://iceberg-rest:8181");
        assertFalse(props.containsKey("s3.endpoint"));
        assertFalse(props.containsKey("io-impl"),
                "io-impl should not be set when no S3 endpoint is configured");
    }

    @Test
    void restCatalog_missingRestUri_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                IcebergSinkFactory.buildCatalogProps(
                        "rest", "s3://warehouse/", "", "", "", ""));
    }

    @Test
    void restCatalog_nullRestUri_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                IcebergSinkFactory.buildCatalogProps(
                        "rest", "s3://warehouse/", "", "", "", null));
    }

    // ── Hadoop catalog properties ─────────────────────────────────────────────

    @Test
    void hadoopCatalog_setsWarehouse() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "hadoop", "file:///tmp/wh", "", "", "", "");
        assertEquals("file:///tmp/wh", props.get("warehouse"));
        assertFalse(props.containsKey("uri"));
        assertFalse(props.containsKey("io-impl"));
    }

    @Test
    void hadoopCatalog_doesNotSetRestUri() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "hadoop", "s3a://wh/", "http://minio:9000", "k", "s", "http://rest:8181");
        assertFalse(props.containsKey("uri"),
                "uri should not appear in hadoop catalog props");
    }

    // ── Unknown catalog type ─────────────────────────────────────────────────

    @Test
    void unknownCatalogType_throwsFromLoader() {
        Map<String, String> props = IcebergSinkFactory.buildCatalogProps(
                "glue", "s3://wh/", "", "", "", "");
        assertThrows(IllegalArgumentException.class, () ->
                IcebergSinkFactory.buildCatalogLoader(
                        "glue", new org.apache.hadoop.conf.Configuration(), props));
    }
}
