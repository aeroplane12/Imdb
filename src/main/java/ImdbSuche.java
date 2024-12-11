import java.sql.*;

public class ImdbSuche {
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Insufficient arguments. Usage: java ImdbSuche -d <database> -s <server> -p <port> -u <username> -pw <password> -k <keyword>");
            return;
        }

        String database = "";
        String server = "";
        String port = "";
        String username = "";
        String password = "";
        String keyword = "";

        for (int i = 0; i < args.length; i += 2) {
            switch (args[i]) {
                case "-d":
                    database = args[i + 1];
                    break;
                case "-s":
                    server = args[i + 1];
                    break;
                case "-p":
                    port = args[i + 1];
                    break;
                case "-u":
                    username = args[i + 1];
                    break;
                case "-pw":
                    password = args[i + 1];
                    break;
                case "-k":
                    keyword = args[i + 1];
                    break;
            }
        }

        // Construct the database URL
        String url = "jdbc:postgresql://" + server + ":" + port + "/" + database;

        // Open a connection
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // Search for movies and actors
            searchMovies(conn, keyword);
            searchActors(conn, keyword);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void searchMovies(Connection conn, String keyword) throws SQLException {
        String query = "SELECT m.title, m.year, " +
                "(SELECT string_agg(DISTINCT g.genre, ', ') FROM genre g WHERE g.movie_id = m.mid) AS genres, " +
                "(SELECT string_agg(DISTINCT TRIM(TRAILING ',' FROM a.name), ',\n') FROM actor a WHERE a.movie_id = m.mid) AS cast, " +
                "(SELECT string_agg(DISTINCT TRIM(TRAILING ',' FROM ac.name), ',\n') FROM actress ac WHERE ac.movie_id = m.mid) AS actresses " +
                "FROM movie m " +
                "WHERE m.title LIKE ? " +
                "ORDER BY m.title";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, "%" + keyword + "%");
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("MOVIES");
                while (rs.next()) {
                    String title = rs.getString("title");
                    int releaseYear = rs.getInt("year");
                    String genres = rs.getString("genres");
                    String cast = rs.getString("cast");
                    String actresses = rs.getString("actresses");

                    System.out.println(title + ", " + releaseYear + ", " + genres);

                    if (actresses != null) {
                        String[] actressNames = actresses.split(",\n");
                        for (String actressName : actressNames) {
                            System.out.println(actressName);
                        }
                    }

                    if (cast != null) {
                        String[] castNames = cast.split(",\n");
                        for (String castName : castNames) {
                            System.out.println(castName);
                        }
                    }

                    System.out.println();
                }
            }
        }
    }


    private static void searchActors(Connection conn, String keyword) throws SQLException {
        String query = "SELECT actor.name AS actor_name, " +
                "STRING_AGG(DISTINCT movie.title, E'\n') AS movies, " +
                "COUNT(DISTINCT coactor.name) AS co_star_count " +
                "FROM actor " +
                "JOIN movie ON actor.movie_id = movie.mid " +
                "JOIN (" +
                "    SELECT actor.movie_id, actor.name, COUNT(*) AS movie_count " +
                "    FROM actor " +
                "    GROUP BY actor.movie_id, actor.name " +
                ") AS coactor ON actor.movie_id = coactor.movie_id AND actor.name <> coactor.name " +
                "WHERE actor.name LIKE ? " +
                "GROUP BY actor.name " +
                "ORDER BY co_star_count DESC, actor_name";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, "%" + keyword + "%");
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("ACTORS");
                while (rs.next()) {
                    String actorName = rs.getString("actor_name");
                    String movies = rs.getString("movies");

                    String coStarsQuery = "SELECT coactor.name, COUNT(*) AS movie_count " +
                            "FROM actor AS coactor " +
                            "JOIN movie ON coactor.movie_id = movie.mid " +
                            "JOIN (SELECT DISTINCT movie.title FROM actor JOIN movie ON actor.movie_id = movie.mid WHERE actor.name = ?) AS actor_movies ON movie.title = actor_movies.title " +
                            "WHERE coactor.name <> ? " +
                            "GROUP BY coactor.name " +
                            "ORDER BY movie_count DESC, coactor.name";

                    try (PreparedStatement coStarsStmt = conn.prepareStatement(coStarsQuery)) {
                        coStarsStmt.setString(1, actorName);
                        coStarsStmt.setString(2, actorName);
                        try (ResultSet coStarsRs = coStarsStmt.executeQuery()) {
                            StringBuilder coStars = new StringBuilder();
                            while (coStarsRs.next()) {
                                String coStarName = coStarsRs.getString("name");
                                int movieCount = coStarsRs.getInt("movie_count");
                                coStars.append(coStarName);
                                coStars.append(" (").append(movieCount).append(")");
                                coStars.append("\n");
                            }

                            System.out.println(actorName);
                            System.out.println("PLAYED IN");
                            System.out.println(movies);
                            System.out.println("CO-STARS");
                            System.out.println(coStars);
                            System.out.println();
                        }
                    }
                }
            }
        }
    }

}


