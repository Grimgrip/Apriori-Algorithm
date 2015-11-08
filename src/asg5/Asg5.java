package asg5;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFileChooser;

public class Asg5 {

	static private boolean debug = true;
	private int minimumSupport;
	private Connection conn;

	public void importAndDataMine() throws Exception {

		conn = null;
		try {
			// Open Db Connection
			conn = getConnection();
			conn.setAutoCommit( false );

			// 1) Get file Name
			String selectedFileName = askUserForFileName();

			// 2) Import data into SQL
			importDataIntoSql( selectedFileName );

			// 3) Apriori
			dataMine();
		}
		catch ( Exception e ) {
			System.err.println( "Error: " + e.getMessage() );
			e.printStackTrace();
		}
		finally {
			conn.close();
			conn = null;
		}
	}

	private void importDataIntoSql( String selectedFileName ) throws SQLException, IOException {
		PreparedStatement pstmt = null;
		try {
			this.createPurchaseTable( conn );
			pstmt = conn.prepareStatement( "insert into Purchase(Customer, ItemBought) values (?, ?)" );

			List<String> lines = Files.readAllLines( Paths.get( selectedFileName ), StandardCharsets.UTF_8 );
			ArrayList<String> customerNames = new ArrayList<String>();
			for ( int i = 1; i < lines.size(); i++ ) {
				String line = lines.get( i );
				String[] fields = line.trim().split( "\\s+" );
				if ( fields.length == 2 ) {
					pstmt.setString( 1, fields[ 0 ] );
					pstmt.setString( 2, fields[ 1 ] );
					pstmt.executeUpdate();

					if ( !customerNames.contains( fields[ 0 ] ) ) {
						customerNames.add( fields[ 0 ] );
					}
				}
			}
			if ( lines.size() > 0 ) {
				String supportLine = lines.get( 0 );
				int customerCount = customerNames.size();
				float supportFloat = Float.parseFloat( supportLine );
				minimumSupport = (int) Math.ceil( supportFloat * customerCount );

				System.out.println( "Minimum support value: " + supportFloat + ", support: " + minimumSupport  );
			}
                        
                        conn.commit();
		}
		catch ( Exception e ) {
			System.err.println( "Error: " + e.getMessage() );
			e.printStackTrace();
		}
		finally {
			if ( pstmt != null ) {
				pstmt.close();
			}
		}
	}

	private String askUserForFileName() {
		//return "Asg5DS.txt";
                final JFileChooser fc = new JFileChooser();
		fc.setCurrentDirectory( new File( "." ) );
                fc.setDialogTitle( "Assignment Five" );
		fc.showSaveDialog( null );

		return fc.getSelectedFile().getName();
	}

	public Connection getConnection() throws Exception {
		DriverManager.registerDriver( new net.sourceforge.jtds.jdbc.Driver() );

                String dataBaseName;
                
                //Connection information redacted
                //Insert own information here
                
                dataBaseName = "";

		String url = "" + dataBaseName;
		String username = "";
		String password = "";
		
                //Class.forName(driver);
		Connection conn = DriverManager.getConnection( url, username, password );
		return conn;
	}

	private void dataMine() throws SQLException {
		// Get L-1 Candidates into C1
		this.dropTable( "F1" );
		this.executeSql( "SELECT ItemBought AS Item1, Count(*) as Count\n" +
						 "INTO F1\n" +
						 "FROM Purchase GROUP BY ItemBought\n" +
						 "HAVING COUNT(*) >= " + minimumSupport  +" \n" +
						 "ORDER BY Item1;" );

		System.out.println( " Result of L-1 itemset: " );
		this.dumpTable( "F1" );

		for (int k = 2; k < 1000; k++  )
		{
			int rows = this.apriori( k );
			if (rows == 0)
			{
				break;
			}
		}


		// Get L2 Candidates (C2) from F1
        // Filter Data with C2 into F2
		// Get L3 Candidates (C3) from F2
	}

	private int apriori( int k ) throws SQLException {

		int rows = 0;

		// Generate candidates
		String Ck1 = "C" + ( k - 1 );
		String Ck = "C" + k;
		String Fk1 = "F" + ( k - 1 );
		String Fk = "F" + k;

		this.dropTable( Ck );
		this.dropTable( Fk );

		generateCandidates( k, Ck, Fk1 );

		rows = calculateOccurrences( k, Ck, Fk );
		return rows;
        }

	private void generateCandidates( int k, String ck, String fk1 ) throws SQLException {
		String columns = "", whereClause = "", fromClause = "", orderByClause = "";

		for ( int i = 1; i <= k - 1; i++ ) {
			if ( i > 1 ) {
				columns += ", ";
                                orderByClause += ", ";
                        }
			columns += "p1.Item" + i + " AS Item" + i;


			if (i < k-2){
				if ( whereClause.length() > 0 ) {
					whereClause += "\n	 AND ";
				}whereClause += "p1.Item" + i + " <> p2.Item" + (i+1);
			}
                        orderByClause += "Item" + i;
		}



		columns += ", p2.Item" + ( k - 1 ) + " AS Item" + k;

		fromClause = fk1 + " p1, " + fk1 + " p2";
		if (k > 1)
		{
			if (whereClause.length() > 0)
			{
				whereClause += " AND ";
			}
			whereClause += "\n	 p1.Item" + (k-1) + " < p2.Item" + ( k - 1 );
		}

		String ckSql = "SELECT DISTINCT " + columns + "\n" +
					   " INTO " + ck + "\n" +
					   " FROM  " + fromClause + "\n" +
					   " WHERE " + whereClause + "\n" +
					   " ORDER BY " + orderByClause;
		//System.out.println( ckSql + "\n\n" );


		this.executeSql( ckSql );

		this.dumpTable( ck );
	}

	private int calculateOccurrences( int k, String ck, String fk ) throws SQLException {

		int rows = 0;

		String columns = "", whereClause = "", groupByClause = "", orderByClause = "";

		for ( int i = 1; i <= k; i++ ) {
			if ( i > 1 ) {
				columns += ", ";
				groupByClause += ", ";
				whereClause += " OR ";
				orderByClause += ", ";
			}
			columns += "Item" + i;
			groupByClause += "Item" + i;
			orderByClause += "Item" + i;


			whereClause += "p.ItemBought = " + ck + ".Item" + i;

		}

		String fkSql = "SELECT " + columns + ", COUNT(*) AS Count \n" +
					   "INTO " + fk + "\n" +
						"FROM\n" +
					   "( SELECT " + columns + ", Customer, Count(*) as Count FROM Purchase p\n" +
					   	"	INNER JOIN " + ck + "\n" +
						 " ON " + whereClause + "\n" +
						 " GROUP BY " + groupByClause + ", Customer\n" +
						 " HAVING COUNT(*) = " + k + ") AS " + fk + "\n" +
						 " GROUP BY " + groupByClause + "\n" +
				"HAVING COUNT(*) >= " + minimumSupport + "\n" +
				"ORDER BY " + orderByClause;

		//System.out.println( fkSql + "\n\n");
		this.executeSql( fkSql );

		System.out.println();
                System.out.println( " Result of L-" + k + " itemset: " );

		rows = this.dumpTable( fk );
		return rows;
         }


	private void dropTable( String tableName ) throws SQLException {

		this.executeSql(
				"if exists\n" +
				"    (select *\n" +
				"     from sysobjects\n" +
				"     where id = object_id(N'" + tableName + "')\n" +
				"           and OBJECTPROPERTY(id, N'IsUserTable') = 1\n" +
				"    )\n" +
				"  drop table " + tableName + "\n" );
	}

	private void createPurchaseTable( Connection conn ) throws SQLException {
		this.dropTable( "Purchase" );
		this.executeSql( "CREATE TABLE Purchase (Customer CHAR(12), ItemBought CHAR(12) )" );
	}

	private void executeSql( String sql ) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute( sql );
			conn.commit();
		}
		catch ( Exception e ) {
			System.err.println( "Error: " + e.getMessage() );
			e.printStackTrace();
		}
		finally {
			if ( stmt != null ) {
				stmt.close();
			}
		}
	}

        private int dumpTable( String tableName ) throws SQLException {
		int rows = 0;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery( "SELECT * FROM " + tableName );

			ResultSetMetaData metaData = rs.getMetaData();
			for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
				System.out.print( metaData.getColumnName( i ) + "	" );
			}
			System.out.println();
			for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
				System.out.print( "---	" );
			}
			System.out.println();

			while ( rs.next() ) {
				for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
					System.out.print( rs.getString( i ) + "	" );
				}
				System.out.println();
				rows++;
			}
		}
		catch ( Exception e ) {
			System.err.println( "Error: " + e.getMessage() );
			e.printStackTrace();
		}
		finally {
			if ( stmt != null ) {
				stmt.close();
			}
		}
		return rows;
	}

        public void run() {
		JButton open = new JButton();
		final JFileChooser fc = new JFileChooser();
		fc.setCurrentDirectory( new java.io.File( "." ) );
		fc.setDialogTitle( "Assignment Five" );
		//fc.setFileSelectionMode(JFileChooser.FILES_ONLY);

		if ( fc.showOpenDialog( open ) == JFileChooser.APPROVE_OPTION ) {

		}

		System.out.println( fc.getSelectedFile().getAbsolutePath() );
	}

	public static void main( String args[] ) throws Exception {
		Asg5 asg = new Asg5();
		asg.importAndDataMine();
        }
}
