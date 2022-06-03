package worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.sql.*;
import org.json.JSONObject;

class Worker {
  public static void main(String[] args) {
    try {
      Jedis redis = connectToRedis("redis");
      
      String dbname = System.getenv().getOrDefault("PG_DBNAME", "postgres");
      String dbhost = System.getenv().getOrDefault("PG_HOST", "db");
      Connection dbConn = connectToDB(dbhost, dbname);

      System.err.println("Watching vote queue");

      while (true) {
        String voteJSON = redis.blpop(0, "votes").get(1);
        JSONObject voteData = new JSONObject(voteJSON);
        String voterID = voteData.getString("voter_id");
        String vote = voteData.getString("vote");

        System.err.printf("Processing vote for '%s' by '%s'\n", vote, voterID);
        updateVote(dbConn, dbname, voterID, vote);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  static void updateVote(Connection dbConn,String dbname, String voterID, String vote) throws SQLException {
    PreparedStatement insert = dbConn.prepareStatement(
      "INSERT INTO "+dbname+".votes (id, vote) VALUES (?, ?)");
    insert.setString(1, voterID);
    insert.setString(2, vote);

    try {
      insert.executeUpdate();
    } catch (SQLException e) {
      PreparedStatement update = dbConn.prepareStatement(
        "UPDATE "+dbname+".votes SET vote = ? WHERE id = ?");
      update.setString(1, vote);
      update.setString(2, voterID);
      update.executeUpdate();
    }
  }

  static Jedis connectToRedis(String host) {
    Jedis conn = new Jedis(host);

    while (true) {
      try {
        conn.keys("*");
        break;
      } catch (JedisConnectionException e) {
        System.err.println("Waiting for redis");
        sleep(1000);
      }
    }

    System.err.println("Connected to redis");
    return conn;
  }

  static Connection connectToDB(String host, String dbname) throws SQLException {
    Connection conn = null;
    try {
      var env = System.getenv();
      String user = env.getOrDefault("PG_USERNAME", "postgres");
      String password = env.getOrDefault("PG_PASSWORD", "postgres");
      String ssl = env.getOrDefault("PG_SSLMODE", "false");
      Class.forName("org.postgresql.Driver");
      var url = db -> "jdbc:postgresql://" + host + "/"+db+"?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl="+ssl;

      while (conn == null) {
        try { 
          conn = DriverManager.getConnection(url(dbname), user , password );          
        } catch (SQLException e) {
          e.printStackTrace();
          System.err.println("Waiting for db");
          sleep(1000);
        }
      }
      String initdb = env.getOrDefault( "INIT_DB" , 
                                        "CREATE TABLE IF NOT EXISTS "+dbname+".votes (id VARCHAR(255) NOT NULL UNIQUE, vote VARCHAR(255) NOT NULL)" );
      PreparedStatement st = conn.prepareStatement(initdb);
      st.executeUpdate();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }    

    System.err.println("Connected to db");
    return conn;
  }

  static void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}
