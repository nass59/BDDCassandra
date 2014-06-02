package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {

    private Cluster cluster;
    private Session session;

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
        // création d'un keyspace nommé "bddCassandra"
        session.execute("CREATE KEYSPACE IF NOT EXISTS bddCassandra WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        // A changer
        session.execute(
                "CREATE TABLE IF NOT EXISTS bddCassandra.essone (code_insee int PRIMARY KEY, code_postal int, commune text, wgs84 text, federation text,licences_en_2011 int, moins_de_20_ans int, entre_20_et_60_ans int, plus_de_60_ans int, femmes int, "
                + "femmes_moins_de_20_ans int, femmes_de_20_a_60_ans int, femmes_plus_de_60_ans int, licences_en_zone_urbaine_sensible_zus int, population_totale_2010 int, population_femme int, population_femmes_de_moins_de_20_ans int, "
                + "population_femme_de_20_a_60_ans int, population_femme_de_plus_de_60_ans int, population_de_moins_de_20_ans int, population_de_20_a_60_ans int, population_de_plus_de_60_ans int);"
        );
        System.out.println(" table essone created");
        System.out.println("...");
        session.execute(
                "CREATE TABLE IF NOT EXISTS bddCassandra.seinesaintdenis (code_insee int PRIMARY KEY, code_postal int, commune text, wgs84 text, federation text,licences_en_2011 int, moins_de_20_ans int, entre_20_et_60_ans int, plus_de_60_ans int, femmes int, "
                + "femmes_moins_de_20_ans int, femmes_de_20_a_60_ans int, femmes_plus_de_60_ans int, licences_en_zone_urbaine_sensible_zus int, population_totale_2010 int, population_femme int, population_femmes_de_moins_de_20_ans int, "
                + "population_femme_de_20_a_60_ans int, population_femme_de_plus_de_60_ans int, population_de_moins_de_20_ans int, population_de_20_a_60_ans int, population_de_plus_de_60_ans int);"
        );
        System.out.println("table seinesaintdenis created");
        System.out.println("...");
        session.execute(
                "CREATE TABLE IF NOT EXISTS bddCassandra.valdemarne (code_insee int PRIMARY KEY, code_postal int, commune text, wgs84 text, federation text,licences_en_2011 int, moins_de_20_ans int, entre_20_et_60_ans int, plus_de_60_ans int, femmes int, "
                + "femmes_moins_de_20_ans int, femmes_de_20_a_60_ans int, femmes_plus_de_60_ans int, licences_en_zone_urbaine_sensible_zus int, population_totale_2010 int, population_femme int, population_femmes_de_moins_de_20_ans int, "
                + "population_femme_de_20_a_60_ans int, population_femme_de_plus_de_60_ans int, population_de_moins_de_20_ans int, population_de_20_a_60_ans int, population_de_plus_de_60_ans int);"
        );
    }

    public void querryPutEssone() {
         session.execute(
                "COPY bddCassandra.essone (code_insee, code_postal, commune, wgs84, federation, licences_en_2011, moins_de_20_ans, entre_20_et_60_ans, plus_de_60_ans, femmes, femmes_moins_de_20_ans, femmes_de_20_a_60_ans, femmes_plus_de_60_ans, licences_en_zone_urbaine_sensible_zus, population_totale_2010, population_femme, population_femmes_de_moins_de_20_ans, population_femme_de_20_a_60_ans, population_femme_de_plus_de_60_ans, population_de_moins_de_20_ans, population_de_20_a_60_ans, population_de_plus_de_60_ans) FROM 'essone.csv' WITH DELIMITER=';' ;"
        ); 
    }
    public void queryEssone() {
        // on execute une requête
        ResultSet results = session.execute("SELECT * FROM bddCassandra.essone ");
        // on affiche le résultat
        System.out.println(String.format("%-10s\t%-10s\t%-20s\t%-70s\t%-30s\t%-30s\t%-30s\t%s-30s", "code_insee", "code_postal", "commune", "federation", "licences_en_2011", "moins_de_20_ans", "entre_20_et_60_ans", "femmes",
                "-------------------------------+-----------------------+--------------------"));
        displayResults(results);
    }

    public void queryValDeMarne() {
        // on execute une requête
        ResultSet results = session.execute("SELECT * FROM bddCassandra.valdemarne ");
        // on affiche le résultat
        System.out.println(String.format("%-10s\t%-10s\t%-20s\t%-70s\t%-30s\t%-30s\t%-30s\t%s-30s", "code_insee", "code_postal", "commune", "federation", "licences_en_2011", "moins_de_20_ans", "entre_20_et_60_ans", "femmes",
                "-------------------------------+-----------------------+--------------------"));
        displayResults(results);
    }

    public void querySeineSaintDenis() {
        // on execute une requête
        ResultSet results = session.execute("SELECT * FROM bddCassandra.seinesaintdenis ");
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

    public void copyData() {
        session.execute(
                "COPY bddCassandra.seinesaintdenis (code_insee, code_postal, commune, wgs84, federation, licences_en_2011, moins_de_20_ans, entre_20_et_60_ans, plus_de_60_ans, femmes, femmes_moins_de_20_ans, femmes_de_20_a_60_ans, femmes_plus_de_60_ans, licences_en_zone_urbaine_sensible_zus, population_totale_2010, population_femme, population_femmes_de_moins_de_20_ans, population_femme_de_20_a_60_ans, population_femme_de_plus_de_60_ans, population_de_moins_de_20_ans, population_de_20_a_60_ans, population_de_plus_de_60_ans) FROM 'seineSaintDenis.csv' WITH DELIMITER=';' ;"
        );
    }

    public static void main(String[] args) {
        // création d'un client
        SimpleClient client = new SimpleClient();
        // connection (localhost)
        client.connect("127.0.0.1");
        // création de la base de données (schema)
         client.createSchema();
        // on interroge la base de données
        //client.copyData();
        client.queryEssone();
        client.queryValDeMarne();
        client.querySeineSaintDenis();
        client.close();
    }
}
