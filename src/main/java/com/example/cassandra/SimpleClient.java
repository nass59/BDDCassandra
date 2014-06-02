package com.example.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class SimpleClient {

    private Cluster cluster;
    private Session session;
    private boolean keyspaceCreated;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void close() {
        cluster.close();
    }

    public void createSchema() {
        // création d'un keyspace nommé "simplex"
        session.execute("CREATE KEYSPACE bddCassandra WITH replication "
                + "= {'class':'NetworkTopologyStrategy', 'replication_factor':2};");
        // creation de la table songs (tags est un tableau)
        // A changer
        session.execute(
                "CREATE TABLE bddCassandra.essone ("
                + "id uuid PRIMARY KEY,"
                + "title text,"
                + "album text,"
                + "artist text,"
                + "tags set<text>,"
                + "data blob"
                + ");");
        keyspaceCreated = true;
    }

    public void queryEssone() {
        // on execute une requête
        ResultSet results = session.execute("SELECT * FROM sport.essone ");
        // on affiche le résultat
        System.out.println(String.format("%-10s\t%-10s\t%-20s\t%-70s\t%-30s\t%-30s\t%-30s\t%s-30s", "code_insee", "code_postal", "commune", "federation", "licences_en_2011", "moins_de_20_ans", "entre_20_et_60_ans", "femmes",
                "-------------------------------+-----------------------+--------------------"));
        displayResults(results);
    }

    public void queryValDeMarne() {
        // on execute une requête
        ResultSet results = session.execute("SELECT * FROM sport.valdemarne ");
        // on affiche le résultat
        System.out.println(String.format("%-10s\t%-10s\t%-20s\t%-70s\t%-30s\t%-30s\t%-30s\t%s-30s", "code_insee", "code_postal", "commune", "federation", "licences_en_2011", "moins_de_20_ans", "entre_20_et_60_ans", "femmes",
                "-------------------------------+-----------------------+--------------------"));
        displayResults(results);
    }

    public void displayResults(ResultSet results) {
        for (Row row : results) {
            System.out.println(String.format("%-10s\t%-10s\t%-20s\t%-70s\t%-30s\t%-30s\t%-30s\t%-30s", row.getInt("code_insee"),
                    row.getInt("code_postal"), row.getString("commune"), row.getString("federation"), row.getInt("licences_en_2011"), row.getInt("moins_de_20_ans"), row.getInt("entre_20_et_60_ans"), row.getInt("femmes")));
        }
        System.out.println();
    }

    public static void main(String[] args) {
        // création d'un client
        SimpleClient client = new SimpleClient();
        // connection (localhost)
        client.connect("127.0.0.1");
        // création de la base de données (schema)
        // client.createSchema();
        // on interroge la base de données
        client.queryEssone();
        client.queryValDeMarne();
        client.close();
    }
}
