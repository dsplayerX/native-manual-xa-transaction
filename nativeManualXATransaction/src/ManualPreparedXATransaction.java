import java.sql.*;
import javax.sql.*;
import javax.swing.plaf.nimbus.State;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.apache.derby.jdbc.EmbeddedXADataSource;

public class ManualPreparedXATransaction {
    public static void main(String[] args) {
        Xid xid = new MyXid("6f9fc418-0f46-4042-88c2-9342dfbdd911", "1");

        try {
            EmbeddedXADataSource dataSource1 = new EmbeddedXADataSource();
            EmbeddedXADataSource dataSource2 = new EmbeddedXADataSource();
            dataSource1.setDatabaseName("../testdb1");
            dataSource1.setCreateDatabase("create");
            dataSource2.setDatabaseName("../testdb2");
            dataSource2.setCreateDatabase("create");

            XAConnection xaConnection1 = ((XADataSource) dataSource1).getXAConnection();
            XAConnection xaConnection2 = ((XADataSource) dataSource2).getXAConnection();

            XAResource xaResource1 = xaConnection1.getXAResource();
            XAResource xaResource2 = xaConnection2.getXAResource();

            Connection connection1 = xaConnection1.getConnection();
            Connection connection2 = xaConnection2.getConnection();


            // Create the table if it doesn't exist
            try {
                Statement createTableStatement1 = connection1.createStatement();
                Statement createTableStatement2 = connection2.createStatement();
                createTableStatement1.execute("CREATE TABLE test (id INT, data VARCHAR(255))");
                createTableStatement1.close();
                createTableStatement2.execute("CREATE TABLE test (id INT, data VARCHAR(255))");
                createTableStatement2.close();
            } catch (SQLException e) {
                // Table already exists
            }

            // manual xa operations
            try {
                connection1.setAutoCommit(false);
                connection2.setAutoCommit(false);
                xaResource1.start(xid, XAResource.TMNOFLAGS);
                xaResource2.start(xid, XAResource.TMNOFLAGS);

                Statement stmt1 = connection1.createStatement();
                stmt1.execute("insert into test values(6, 'cc')");
                stmt1.close();

                Statement stmt2 = connection2.createStatement();
                stmt2.execute("insert into test values(7, 'dd')");
                stmt2.close();

                xaResource1.end(xid, XAResource.TMSUCCESS);
                xaResource2.end(xid, XAResource.TMSUCCESS);

                int prepareStatus1 = xaResource1.prepare(xid);
                int prepareStatus2 = xaResource2.prepare(xid);

                System.out.println("Prepare 1 status: " + (prepareStatus1 == XAResource.XA_OK ? "OK" : "FAIL"));
                System.out.println("Prepare 2 status: " + (prepareStatus2 == XAResource.XA_OK ? "OK" : "FAIL"));
            } finally {
                connection1.close();
                connection2.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // commit the transaction manually from xid
//        try {
//            EmbeddedXADataSource dataSource2 = new EmbeddedXADataSource();
//            dataSource2.setDatabaseName("../testdb");
//            dataSource2.setCreateDatabase("false");
//
//            XADataSource xaDataSource2 = dataSource2;
//
//            XAConnection xaConnection2 = xaDataSource2.getXAConnection();
//
//            XAResource xaResource2 = xaConnection2.getXAResource();
//
//            xaResource2.commit(xid, false);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        // view inserted data
//        try {
//            EmbeddedXADataSource dataSource = new EmbeddedXADataSource();
//            dataSource.setDatabaseName("../testdb");
//            dataSource.setCreateDatabase("false");
//
//            XADataSource xaDataSource = dataSource;
//
//            XAConnection xaConnection = xaDataSource.getXAConnection();
//
//            Connection connection = xaConnection.getConnection();
//
//            Statement stmt = connection.createStatement();
//            ResultSet rs = stmt.executeQuery("select * from test");
//            while (rs.next()) {
//                System.out.println("id: " + rs.getInt(1) + ", data: " + rs.getString(2));
//            }
//            rs.close();
//            stmt.close();
//            connection.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}

class MyXid implements Xid {
    private int formatId;
    private byte gtrid[];
    private byte bqual[];

    public MyXid(String trxid, String branchid) {
        formatId = ('B' << 24) + ('A' << 16) + ('L' << 8); // unique for ballerina transactions
        gtrid = trxid.getBytes();
        bqual = branchid.getBytes();
    }

    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        return gtrid;
    }

    public byte[] getBranchQualifier() {
        return bqual;
    }
}
