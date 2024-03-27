import java.sql.*;
import javax.sql.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.apache.derby.jdbc.EmbeddedXADataSource;

public class ManualPreparedXATransaction {
    public static void main(String[] args) {
        Xid xid = new MyXid(10);

        try {
            EmbeddedXADataSource dataSource = new EmbeddedXADataSource();
            dataSource.setDatabaseName("mydatabase");
            dataSource.setCreateDatabase("create");

            XADataSource xaDataSource = dataSource;

            XAConnection xaConnection = xaDataSource.getXAConnection();

            XAResource xaResource = xaConnection.getXAResource();

            Connection connection = xaConnection.getConnection();

            // Create the table if it doesn't exist
//            Statement createTableStatement = connection.createStatement();
//            createTableStatement.execute("CREATE TABLE IF NOT EXISTS test (id INT, data VARCHAR(255))");
//            createTableStatement.close();

            // Your XA transaction operations
            try {
                connection.setAutoCommit(false);
                xaResource.start(xid, XAResource.TMNOFLAGS);

                Statement stmt = connection.createStatement();
                stmt.execute("insert into test values(6, 'cc')");
                stmt.close();

                xaResource.end(xid, XAResource.TMSUCCESS);
                int prepareStatus = xaResource.prepare(xid);
                System.out.println("Prepare status: " + (prepareStatus == XAResource.XA_OK ? "OK" : "FAIL"));

            } finally {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // commit the transaction manually from xid
        try {
            EmbeddedXADataSource dataSource2 = new EmbeddedXADataSource();
            dataSource2.setDatabaseName("mydatabase");
            dataSource2.setCreateDatabase("false");

            XADataSource xaDataSource2 = dataSource2;

            XAConnection xaConnection2 = xaDataSource2.getXAConnection();

            XAResource xaResource2 = xaConnection2.getXAResource();

            xaResource2.commit(xid, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // view inserted data
        try {
            EmbeddedXADataSource dataSource = new EmbeddedXADataSource();
            dataSource.setDatabaseName("mydatabase");
            dataSource.setCreateDatabase("false");

            XADataSource xaDataSource = dataSource;

            XAConnection xaConnection = xaDataSource.getXAConnection();

            XAResource xaResource = xaConnection.getXAResource();

            Connection connection = xaConnection.getConnection();

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from test");
            while (rs.next()) {
                System.out.println("id: " + rs.getInt(1) + ", data: " + rs.getString(2));
            }
            rs.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MyXid implements Xid {
    private int formatId;
    private byte gtrid[];
    private byte bqual[];

    public MyXid(int id) {
        formatId = 1;
        gtrid = new byte[] { 0x44, 0x44, 0x44 };
        bqual = new byte[] { (byte) id };
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
