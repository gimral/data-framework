package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class DefaultSchemaRegistryClientDecorator implements SchemaRegistryClient {
    protected SchemaRegistryClient decoratedClient;
    public DefaultSchemaRegistryClientDecorator(SchemaRegistryClient decoratedClient){
        this.decoratedClient = decoratedClient;
    }

    public int register(String subject, Schema schema) throws IOException, RestClientException {
        return decoratedClient.register(subject, schema);
    }

    public int register(String subject, Schema schema, int version, int id) throws IOException,
            RestClientException {
        return decoratedClient.register(subject, schema, version, id);
    }

    @Deprecated
    public Schema getByID(int id) throws IOException, RestClientException {
        return decoratedClient.getById(id);
    }

    public Schema getById(int id) throws IOException, RestClientException {
        return decoratedClient.getById(id);
    }

    @Deprecated
    public Schema getBySubjectAndID(String subject, int id) throws IOException, RestClientException {
        return decoratedClient.getBySubjectAndId(subject, id);
    }

    public Schema getBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        return decoratedClient.getBySubjectAndId(subject,id);
    }

    public SchemaMetadata getLatestSchemaMetadata(String subject)
            throws IOException, RestClientException {
        return decoratedClient.getLatestSchemaMetadata(subject);
    }

    public SchemaMetadata getSchemaMetadata(String subject, int version)
            throws IOException, RestClientException {
        return decoratedClient.getSchemaMetadata(subject, version);
    }

    public int getVersion(String subject, Schema schema) throws IOException, RestClientException {
        return decoratedClient.getVersion(subject,schema);
    }

    public List<Integer> getAllVersions(String subject) throws IOException, RestClientException {
        return decoratedClient.getAllVersions(subject);
    }

    public boolean testCompatibility(String subject, Schema schema)
            throws IOException, RestClientException {
        return decoratedClient.testCompatibility(subject,schema);
    }

    public String updateCompatibility(String subject, String compatibility)
            throws IOException, RestClientException {
        return decoratedClient.updateCompatibility(subject,compatibility);
    }

    public String getCompatibility(String subject) throws IOException, RestClientException {
        return decoratedClient.getCompatibility(subject);
    }

    public String setMode(String mode)
            throws IOException, RestClientException {
        return decoratedClient.setMode(mode);
    }

    public String setMode(String mode, String subject)
            throws IOException, RestClientException {
        return decoratedClient.setMode(mode,subject);
    }

    public String getMode() throws IOException, RestClientException {
        return decoratedClient.getMode();
    }

    public String getMode(String subject) throws IOException, RestClientException {
        return decoratedClient.getMode(subject);
    }

    public Collection<String> getAllSubjects() throws IOException, RestClientException {
        return decoratedClient.getAllSubjects();
    }

    public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
        return decoratedClient.deleteSubject(subject);
    }

    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
            throws IOException, RestClientException {
        return decoratedClient.deleteSubject(requestProperties,subject);
    }

    public Integer deleteSchemaVersion(String subject, String version)
            throws IOException, RestClientException {
        return decoratedClient.deleteSchemaVersion(subject,version);
    }

    public Integer deleteSchemaVersion(
            Map<String, String> requestProperties,
            String subject,
            String version)
            throws IOException, RestClientException {
        return decoratedClient.deleteSchemaVersion(requestProperties, subject, version);
    }

    public void reset() {
        decoratedClient.reset();
    }

    public int getId(String subject, Schema schema) throws IOException, RestClientException {
        return decoratedClient.getId(subject, schema);
    }
}
