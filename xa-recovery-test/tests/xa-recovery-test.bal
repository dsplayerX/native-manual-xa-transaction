import ballerina/io;
import ballerina/sql;
import ballerina/test;

import ballerinax/java.jdbc;

final jdbc:Client dbClient1 = check new (url = "jdbc:derby:../testdb1");
final jdbc:Client dbClient2 = check new (url = "jdbc:derby:../testdb2");

string mockTransactionLogRecord = "6f9fc418-0f46-4042-88c2-9342dfbdd911:1|COMMITTING|1709872462331";

function prepare() returns error? {
    // create tables if not exists
    _ = check dbClient1->execute(`CREATE TABLE IF NOT EXISTS EMPLOYEE (ID INT, NAME VARCHAR(30))`);
    _ = check dbClient2->execute(`CREATE TABLE IF NOT EXISTS SALARY (ID INT, "VALUE" FLOAT)`);

    // create a log file
    check io:fileWriteString("xa-transactions/recoveryLog.log", mockTransactionLogRecord);
}

@test:Config {
    enable: false
}
function testXATransactionRecovery() returns error? {
    check prepare();

    string str = "";

    transaction {
        str += "transaction started";

        _ = check dbClient1->execute(`INSERT INTO EMPLOYEE VALUES (2, 'John')`);
        _ = check dbClient2->execute(`INSERT INTO SALARY VALUES (2, 20000.00)`);

        var commitResult = commit;
        if commitResult is () {
            str += " -> transaction committed";
        } else {
            str += " -> transaction failed";
        }
        str += " -> transaction ended.";
    }

    test:assertEquals(str, "transaction started -> transaction committed -> transaction ended.");

    // Verify that the failed transaction data was inserted successfully to both databases
    sql:ExecutionResult employeeResult = check dbClient1->queryRow(`SELECT * FROM EMPLOYEE WHERE ID = 1`);
    sql:ExecutionResult salaryResult = check dbClient2->queryRow(`SELECT * FROM SALARY WHERE ID = 1`);
    json employeeResultJson = employeeResult.toJson();
    json salaryResultJson = salaryResult.toJson();

    test:assertEquals(employeeResultJson.ID, 1);
    test:assertEquals(employeeResultJson.NAME, "Failure");
    test:assertEquals(salaryResultJson.ID, 1);
    test:assertEquals(salaryResultJson.VALUE, 10000.00);

    checkpanic dbClient1.close();
    checkpanic dbClient2.close();
}
