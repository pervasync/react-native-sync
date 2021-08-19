import util from "./util.js"
/*
Realm supports the following basic types: bool, int, float, double, string, data, and date.
bool properties map to JavaScript boolean values
int, float, and double properties map to JavaScript number values. 
Internally ‘int’ and ‘double’ are stored as 64 bits while float is stored with 32 bits.
string properties map to string
data properties map to ArrayBuffer
date properties map to Date
*/
let getDeviceColDef = function (serverDbType, column) {

    let colDef = null;
    let colType = column.typeName;
    console.log("colType: " + colType);
    if (!colType) {
        console.log("empty colType");
        colType = "";
    }
    colType = colType.toUpperCase();

    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];

    if (serverDbType == "ORADB") {
        colDef = oracleToRealm(column, colType);
    } else if (serverDbType == "MYSQL") {
        colDef = mysqlToRealm(column, colType);
    } else if (serverDbType == "MSSQL") {
        colDef = mssqlToRealm(column, colType);
    } else if (serverDbType == "POSTGRESQL") {
        colDef = postgresqlToRealm(column, colType);
    } else {
        throw new Error("serverDbType not supported: " + serverDbType);
    }

    // this needs to go before calling stringToColObj
    column.deviceColDef = colDef;

    if (colDef.default) {
        try {
            colDef.default = stringToColObj(colDef.default, serverDbType, column);
        } catch (error) {
            console.log("Ignored error parsing default value. colType=" + colType
                + ", Value=" + colDef.default + ", error=" + error);
            // TODO: remove throw
            throw error;
        }
    }

    return colDef;
}

let mysqlToRealm = function (column, colType) {
    let colDef = {};
    colDef.default = column.defaultValue;

    if ("TINYINT" == colType) { // 1 bytes, TINYINT(1) == boolean
        if (column.columnSize < 2) {
            colType = "bool";
        } else {
            colType = "int";
        }
    } else if ("SMALLINT" == colType) { // 2 bytes
        colType = "int";
    } else if ("MEDIUMINT" == colType) { // 3 bytes
        colType = "int";
    } else if ("INT" == colType ||
        "INTEGER" == colType) { // 4 bytes
        colType = "int";
    } else if ("BIGINT" == colType) { // 8 bytes
        colType = "int";
    } else if ("DOUBLE" == colType ||
        "DOUBLE PRECISION" == colType ||
        "REAL" == colType) { // 8 bytes
        colType = "double";
    } else if ("FLOAT" == colType) {
        colType = "float";
    } else if ("DECIMAL" == colType || "DEC" == colType || "NUMERIC" == colType) { //
        if (column.decimalDigits > 0) {
            colType = "double";
        } else {
            colType = "int";
        }
    } else if ("BIT" == colType) { //
        colType = "bool"
    } else if ("DATE" == colType || "DATETIME" == colType ||
        "TIME" == colType || "TIMESTAMP" == colType) {
        colType = "date";
    } else if ("YEAR" == colType) { //
        colType = "string";
    } else if (isBlob("MYSQL", column)) {
        colType = "data";
    } else if (isClob("MYSQL", column)) {
        colType = "string";
    } else if ("CHAR" == colType) { //
        colType = "string";
    } else if ("VARCHAR" == colType) { //
        colType = "string";
    } else if (colType.indexOf("SET") > -1 || colType.indexOf("ENUM") > -1) { //
        colType = "string";
    } else {
        colType = "string";
    }

    colDef.type = colType;

    if (column.nullable) {
        colDef.optional = true;
    } else {
        colDef.optional = false;
    }

    return colDef;
}

let oracleToRealm = function (column, colType) {
    let colDef = {};
    colDef.default = column.defaultValue;

    if ("NUMBER" == colType || "DECIMAL" == colType ||
        "NUMERIC" == colType) {
        if (column.decimalDigits > 0) {
            colType = "double";
        } else {
            colType = "int";
        }
    } else if ("DATE" == colType || "TIMESTAMP" == colType) {
        colType = "date";
    } else if ("VARCHAR2" == colType || "NVARCHAR2" == colType ||
        "VARCHAR" == colType ||
        "CHAR VARYING" == colType ||
        "CHARACTER VARYING" == colType ||
        "NVARCHAR" == colType ||
        "NCHAR VARYING" == colType ||
        "NATIONAL CHAR VARYING" == colType ||
        "NATIONAL CHARACTER VARYING" == colType) {
        colType = "string";
    } else if ("CHAR" == colType || "NCHAR" == colType ||
        "CHARACTER" == colType ||
        "NATIONAL CHAR" == colType ||
        "NATIONAL CHARACTER" == colType) {
        colType = "string";
    } else if (isClob("ORADB", column)) {
        colType = "string";
    } else if (isBlob("ORADB", column)) {
        colType = "data";
    } else if ("RAW" == colType) {
        colType = "data";
    } else if ("INTEGER" == colType || "INT" == colType ||
        "SMALLINT" == colType) {
        colType = "int";
    } else if ("DOUBLE PRECISION" == colType ||
        "REAL" == colType) {
        colType = "double";
    } else if ("FLOAT" == colType) {
        colType = "float";
    } else {
        colType = "string";
    }

    colDef.type = colType;

    if (column.nullable) {
        colDef.optional = true;
    } else {
        colDef.optional = false;
    }

    return colDef;
}

let mssqlToRealm = function (column, colType) {
    let colDef = {};
    colDef.default = column.defaultValue;

    // strip parenthesis off "(x)" or "((x))"
    if (colDef.default != null) {
        colDef.default = colDef.default.trim();
        if (colDef.default.startsWith("((") && colDef.default.endsWith("))")) {
            colDef.default = colDef.default.substring(2, colDef.default.length - 2);
        } else if (colDef.default.startsWith("(") && colDef.default.endsWith(")")) {
            colDef.default = colDef.default.substring(1, colDef.default.length - 1);
        }
        if (colDef.default.toUpperCase().startsWith("N'") && colDef.default.endsWith("'")) {
            colDef.default = colDef.default.substring(1);
        }
    }

    if (isBlob("MSSQL", column)) {
        colType = "data";
    } else if (isClob("MSSQL", column)) {
        colType = "string";
    } else if (colType.endsWith("IDENTITY")) { // identity
        colType = "string";
    } else if ("BIT" == (colType)) { // bit
        colType = "int";
    } else if ("TINYINT" == (colType)) { // 1 bytes
        colType = "int";
    } else if ("SMALLINT" == (colType)) { // 2 bytes
        colType = "int";
    } else if ("MEDIUMINT" == (colType)) { // 3 bytes, non exist for MSSql
        colType = "int";
    } else if ("INT" == (colType)
        || "INTEGER" == (colType)) { // 4 bytes
        colType = "int";
    } else if ("BIGINT" == (colType)) { // 8 bytes
        colType = "int";
    } else if ("REAL" == (colType)) { // Java Float
        colType = "float";
    } else if ("FLOAT" == (colType)) {// Java Double
        colType = "double";
    } else if ("DECIMAL" == (colType) || "MONEY" == (colType)
        || "SMALLMONEY" == (colType)
        || "NUMERIC" == (colType)) { //
        if (column.decimalDigits > 0) {
            colType = "double";
        } else {
            colType = "int";
        }
    } else if ("DATE" == (colType) || "DATETIME" == (colType)
        || "TIME" == (colType) || "DATETIME2" == (colType)) {
        colDef = "date";
    } else if ("CHAR" == (colType)) {
        colType = "string";
    } else if ("NCHAR" == (colType)) {
        colType = "string";
    } else if ("VARCHAR" == (colType) || "NVARCHAR" == (colType)) { // 
        colType = "string";
    } else if ("BINARY" == (colType) || "VARBINARY" == (colType)
        || "TIMESTAMP" == (colType) || "ROWVERSION" == (colType)) { // 
        colType = "data";
        colDef.default = null;
    } else if ("UNIQUEIDENTIFIER" == (colType)) {// uniqueidentifier
        colType = "string";
        colDef.default = null;
    } else if ("SQL_VARIANT" == (colType) || "TABLE" == (colType) || "HIERARCHYID" == (colType)) {
        colType = "data";
        colDef.default = null;
    } else {
        colType = "string";
    }

    colDef.type = colType;

    if (column.nullable) {
        colDef.optional = true;
    } else {
        colDef.optional = false;
    }

    return colDef;
}

let postgresqlToRealm = function (column, colType) {
    let colDef = {};
    colDef.default = column.defaultValue;

    if ("BOOLEAN" == (colType) || "BOOL" == (colType)) { // 2 bytes
        colType = "bool";
    } else if ("SMALLINT" == (colType) || "INT2" == (colType)) { // 2 bytes
        colType = "int";
    } else if ("INTEGER" == (colType) || "INT4" == (colType)) { // 4 bytes
        colType = "int";
    } else if ("BIGINT" == (colType) || "INT8" == (colType)) { // 8 bytes
        colType = "int";
    } else if ("SERIAL" == (colType)) { // 4 bytes
        colType = "int";
        colDef.default = null; // default to implicit sequence value
    } else if ("BIGSERIAL" == (colType)) { // 8 bytes
        colType = "int";
        colDef.default = null; // default to implicit sequence value
    } else if ("REAL" == (colType) || "FLOAT4" == (colType)) { // 8 bytes
        colType = "float";
    } else if ("DOUBLE PRECISION" == (colType) || "FLOAT8" == (colType)) {
        colType = "double";
    } else if ("DECIMAL" == (colType)
        || "NUMERIC" == (colType)) { //
        if (column.decimalDigits > 0) {
            colType = "double";
        } else {
            colType = "int";
        }
    } else if ("MONEY" == (colType)) {
        colType = "double";
    } else if ("BIT" == (colType) || "BIT VARYING" == (colType)) { //
        colType = "int";
        colDef.default = null;
    } else if ("DATE" == (colType)
        || "TIME" == (colType) || "TIMESTAMP" == (colType)) {
        colType = "date";
        colDef.default = null;
    } else if (colType.startsWith("INTERVAL")) { // 
        colType = "string";
    } else if ("BYTEA" == (colType)) {
        colType = "data";
        colDef.default = null;
    } else if ("TEXT" == (colType) || "XML" == (colType)) { // 
        colType = "string";
    } else if (isBlob("POSTGRESQL", column)) {
        colType = "data";
        colDef.default = null;
    } else if (isClob("POSTGRESQL", column)) {
        colType = "string";
        colDef.default = null;
    } else if ("CHAR" == (colType) || "CHARACTER" == (colType) || "BPCHAR" == (colType)) { // 
        colType = "string";
    } else if ("VARCHAR" == (colType) || "CHARACTER VARYING" == (colType)) { // 
        colType = "string";
    } else if (colType.startsWith("ENUM")) { //
        colType = "string";
    } else {
        colType = "string";
    }

    colDef.type = colType;
    if (colDef.default != null) {
        colDef.default = colDef.default;
    }

    if (column.nullable) {
        colDef.optional = true;
    } else {
        colDef.optional = false;
    }

    return colDef;
}

let stringToColObj = function (str, serverDbType, column) {
    if(!str){
        return null;
    }

    if (column.deviceColDef.type == "bool") {
        if ("false" == str || 0 == str || "" == str) {
            return false;
        } else {
            return true
        }
    } else if (column.deviceColDef.type == "int"
        || column.deviceColDef.type == "float"
        || column.deviceColDef.type == "double") {
        let num = Number(str);
        if (isNaN(num)) {
            throw new Error("Not a number. Value=" + str + ". columnName=" + column.columnName
                + ", typeName=" + column.typeName);
        }
        return num;
    } else if (column.deviceColDef.type == "date") {
        //console.log("date before str=");
        if (str.length < 19) {
            let dateOnly = false;
            let timeOnly = false;
            let colType = column.typeName;
            if (!colType) {
                colType = "";
            }
            if ("TIME" == colType) {
                timeOnly = true;
            }
            if ("DATE" == colType && serverDbType != "ORADB") {
                dateOnly = true;
            }

            if (dateOnly) {
                str += "T00:00:00"
            }
            if (timeOnly) {
                str = "1970-01-01T" + str;
            }
        }

        str = str.replace(" ", "T");
        if (!str.endsWith("Z")) {
            str += "Z";
        }
        //console.log("date after str=");
        let obj = new Date(str);
        return obj;
    } else if (column.deviceColDef.type == "data") {
        return util.hex2bytes(str);
    } else {//if(column.deviceColDef.type == "string"){
        return str;
    }
}

let colObjToString = function (obj, serverDbType, column) {
    if (column.deviceColDef.type == "bool") {
        if (!obj) {
            return "false";
        } else {
            return "true"
        }
    } else if (column.deviceColDef.type == "int") {
        return String(obj);
    } else if (column.deviceColDef.type == "float") {
        return String(obj);
    } else if (column.deviceColDef.type == "double") {
        return String(obj);
    } else if (column.deviceColDef.type == "date") {
        if (!obj) {
            return null;
        }
        let str = obj.toISOString();
        str = str.replace(/T/ig, " ");
        str = str.replace(/Z/ig, "");

        let dateOnly = false;
        let timeOnly = false;
        let colType = column.typeName;
        if (!colType) {
            colType = "";
        }
        if ("TIME" == colType) {
            timeOnly = true;
        }
        if ("DATE" == colType && serverDbType != "ORADB") {
            dateOnly = true;
        }

        if (dateOnly || timeOnly) {
            let parts = str.split(" ");
            if (dateOnly) {
                str = parts[0];
            } else {
                str = parts[1];
            }
        }
        return str;
    } else if (column.deviceColDef.type == "data") {
        return util.buf2hex(obj);
    } else {//if(column.deviceColDef.type == "string"){
        return String(obj);
    }
}

let isBlob = function (serverDbType, column) {
    let colType = column.typeName.toUpperCase();
    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];
    if (serverDbType == "MYSQL") {
        if ("TINYBLOB" == colType ||
            "MEDIUMBLOB" == colType ||
            "BLOB" == colType || "LONGBLOB" == colType) {
            //console.log("isBlob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "ORADB") {
        if ("BLOB" == colType || "BFILE" == colType) {
            //console.log("isBlob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "MSSQL") {
        if ("LONGVARBINARY" == column.dataType) {
            return true;
        } else if ("IMAGE" == colType || "VARBINARY" == colType && column.columnSize > 8000) {
            return true;
        }
    } else if (serverDbType == "POSTGRESQL") {
        if ("OID" == colType) {
            return true;
        }
    }

    return false;
}

let isClob = function (serverDbType, column) {
    let colType = column.typeName.toUpperCase();
    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];

    if (serverDbType == "MYSQL") {
        if ("TINYTEXT" == colType || "MEDIUMTEXT" == colType ||
            "TEXT" == colType || "LONGTEXT" == colType) {
            //console.log("isClob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "ORADB") {
        if ("CLOB" == colType || "NCLOB" == colType) {
            //console.log("isClob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "MSSQL") {
        if ("LONGVARCHAR" == column.dataType || "LONGNVARCHAR" == column.dataType
            || "SQLXML" == column.dataType) {
            return true;
        } else if ("TEXT" == colType || "NTEXT" == colType || "XML" == colType) {
            return true;
        } else if (("VARCHAR" == colType || "NVARCHAR" == colType) && column.columnSize > 8000) {
            return true;
        }
    } else if (serverDbType == "POSTGRESQL") {
        /* No types support CLOB API in POSTGRESQL. Well maybe an OID column which is handled by BLOB API
        }*/
    }

    return false;
}

// https://github.com/realm/realm-js/issues/1188
// In some cases the listener may be called when the transaction starts—if the Realm is 
// advanced to the latest version, or Realm entities being observed were modified or 
// deleted in a way that triggers notifications. In those cases, the listener runs 
// within the context of the current write transaction, so an attempt to begin a new write 
// transaction within the notification handler will throw an exception. 
// You can use the Realm.isInTransaction property to determine if your code is executing 
// within a write transaction.
function safeWrite( realm, funWrite ){
    if( realm.isInTransaction ){
        setTimeout( ()=>{
            safeWrite( realm, funWrite );
        }, 50 );

    } else {
        funWrite();
    }
}
export default {
    getDeviceColDef,
    stringToColObj,
    colObjToString,
    isBlob,
    isClob,
    safeWrite
}