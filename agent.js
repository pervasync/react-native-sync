import Realm from 'realm'

import context from './context.js'
import transport from './transport.js'
import db from './db.js'
import fs from './fs.js'
import util from './util.js'

let clientSchemaList = [];
let clientSchemaMap = {};
let clientSchemaSubList = [];
let clientSchemaSubMap = {};

let clientFolderList = [];
let clientFolderMap = {};
let clientFolderSubList = [];
let clientFolderSubMap = {};


let schemaNameIdMap = {}; // schemaName->schemaId
let realmMap = {}; // schemaId->realm

let pvcAdminRealm;
let syncSummary = {};

let syncClientId = -1;
let syncServerId = 0;
let transactionId = 0;

let nidList = [];
let nid = 0;

context.onSyncStateChange("READY");

/**
 * Retrieve and process schema metadata
 */

async function init() {
    console.log("begin agent init");

    // clear metadata cache; do not just assign to {} and [] because of async
    clientSchemaList.splice(0, clientSchemaList.length);
    for (let key in clientSchemaMap) {
        delete clientSchemaMap[key]
    }
    clientSchemaSubList.splice(0, clientSchemaSubList.length);
    for (let key in clientSchemaSubMap) {
        delete clientSchemaSubMap[key]
    }

    clientFolderList.splice(0, clientFolderList.length);
    for (let key in clientFolderMap) {
        delete clientFolderMap[key]
    }
    clientFolderSubList.splice(0, clientFolderSubList.length);
    for (let key in clientFolderSubMap) {
        delete clientFolderSubMap[key]
    }

    pvcAdminRealm = context.pvcAdminRealm;
    let realmSyncSchemas = pvcAdminRealm.objects("pvc__sync_schemas");
    for (let syncSchemaRow of realmSyncSchemas) {
        await initSyncSchema(syncSchemaRow);
    }
    let realmSyncFolders = pvcAdminRealm.objects("pvc__sync_folders");
    for (let syncFolderRow of realmSyncFolders) {
        await initSyncFolder(syncFolderRow);
    }

    console.log("end agent init");
}

async function destroy() {
    console.log("begin agent destroy");

    // clear metadata cache; do not just assign to {} and [] because of async
    clientSchemaList.splice(0, clientSchemaList.length);
    for (let key in clientSchemaMap) {
        delete clientSchemaMap[key]
    }
    clientSchemaSubList.splice(0, clientSchemaSubList.length);
    for (let key in clientSchemaSubMap) {
        delete clientSchemaSubMap[key]
    }

    clientFolderList.splice(0, clientFolderList.length);
    for (let key in clientFolderMap) {
        delete clientFolderMap[key]
    }
    clientFolderSubList.splice(0, clientFolderSubList.length);
    for (let key in clientFolderSubMap) {
        delete clientFolderSubMap[key]
    }

    // close realms
    for (let key in realmMap) {
        realmMap[key].close();
        delete realmMap[key]
    }

    console.log("end agent destroy");
}

function getRealm(schemaName) {
    let schemaId = schemaNameIdMap[schemaName];
    return realmMap[schemaId];
}

function getPath(folderName) {
    let path = null;
    for (let syncFolder of clientFolderList) {
        if (syncFolder.name == folderName) {
            path = context.settings.path + "/files/" + syncFolder.clientFolderPath;
        }
    }
    return path;
}

/**
* Start a synchronization session.
* @param syncDirection Sync direction.
* Valid values are REFRESH_ONLY, CHECK_IN_ONLY and
* TWO_WAY. If null, defaults to TWO_WAY.
* @param syncSchemas List of sync schema names to sync.
* To sync all, use a null or empty syncSchemaNames.
* @returns A promise of SyncSummary object
* @throws An error if there is already an active sync session
*/
async function sync(syncDirection, syncSchemas, syncFolders) {
    // TODO remove
    context.syncing = false;

    if (!context.syncing) {
        try {
            // reset syncSummary
            for (let key in syncSummary) {
                delete syncSummary[key]
            }

            console.log("syncServerUrl=" + context.settings.syncServerUrl);
            console.log("syncUserName=" + context.settings.syncUserName);
            console.log("syncDeviceName=" + context.settings.syncDeviceName);

            let clientPropertiesRows = pvcAdminRealm.objects("pvc__sync_client_properties");
            for (let clientPropertiesRow of clientPropertiesRows) {
                if (clientPropertiesRow["NAME"] == "pervasync.client.id") {
                    syncClientId =
                        Number(clientPropertiesRow["VALUE"]);
                }
                if (clientPropertiesRow["NAME"] == "pervasync.server.id") {
                    syncServerId =
                        Number(clientPropertiesRow["VALUE"]);
                }
                if (clientPropertiesRow["NAME"] == "pervasync.transaction.id") {
                    transactionId =
                        Number(clientPropertiesRow["VALUE"]);
                }
            }

            console.log("syncClientId=" + syncClientId);
            console.log("syncServerId=" + syncServerId);
            console.log("transactionId=" + transactionId);

            if (!syncDirection) {
                syncDirection = "TWO_WAY";
            }

            // sync summary
            syncSummary.syncBeginTime = new Date().getTime();
            syncSummary.checkInDIU_requested = [0, 0, 0];
            syncSummary.checkInDIU_done = [0, 0, 0];
            syncSummary.refreshDIU_requested = [0, 0, 0];
            syncSummary.refreshDIU_done = [0, 0, 0];
            syncSummary.hasDefChanges = false;
            syncSummary.hasDataChanges = false;
            syncSummary.errorCode = -1;
            syncSummary.checkInStatus = "NOT_AVAILABLE";
            syncSummary.checkInSchemaNames = [];
            syncSummary.refreshSchemaNames = [];
            syncSummary.checkInFolderNames = [];
            syncSummary.refreshFolderNames = [];

            syncSummary.refreshStatus = "NOT_AVAILABLE";
            syncSummary.serverSnapshotAge = -1;

            syncSummary.user = context.settings.syncUserName;
            syncSummary.device = context.settings.syncDeviceName;
            syncSummary.syncDirection = syncDirection;
            syncSummary.syncErrorMessages = "";
            syncSummary.syncErrorStacktraces = "";

            let setSchemaFolderNames = function () {
                //console.log("Determine sync schemas and folders");
                if (!syncSchemas || syncSchemas.length == 0) {
                    syncSummary.syncSchemaNames = [];
                    for (let schemaId in clientSchemaMap) {
                        syncSummary.syncSchemaNames.push(clientSchemaMap[schemaId].name);
                    }
                } else {
                    syncSummary.syncSchemaNames = syncSchemas;
                }
                console.log("syncSummary.syncSchemaNames: " + syncSummary.syncSchemaNames.join());

                if (!syncFolders || syncFolders.length == 0) {
                    syncSummary.syncFolderNames = [];
                    for (let folderId in clientFolderMap) {
                        syncSummary.syncFolderNames.push(clientFolderMap[folderId].name);
                    }
                } else {
                    syncSummary.syncFolderNames = syncFolders;
                }
                console.log("syncSummary.syncFolderNames: " + syncSummary.syncFolderNames.join());
            }

            setSchemaFolderNames();
            await send();
            await receive();

            let hasDefChanges = false;
            if (syncSummary.hasDefChanges) {
                console.log("Will sync again since last sync only refreshed def changes.");
                hasDefChanges = true;
                setSchemaFolderNames();
                await send();
                await receive();
            }
            if (syncSummary.hasDefChanges) {
                console.log("Will sync again since last sync only refreshed def changes.");
                hasDefChanges = true;
                setSchemaFolderNames();
                await send();
                await receive();
            }
            syncSummary.hasDefChanges = hasDefChanges;
            syncSummary.hasDataChanges = syncSummary.refreshDIU_done.reduce((sum, item) => sum + item) > 0;
        } catch (e) {
            syncSummary.syncException = e;
            syncSummary.syncErrorMessages += e;
            context.onSyncStateChange("FAILED", syncSummary);
        }

        syncSummary.syncEndTime = new Date().getTime();
        syncSummary.syncDuration = (syncSummary.syncEndTime - syncSummary.syncBeginTime) / 1000.00 + " seconds";

        if (syncSummary.syncException) {
            if (context.syncState != "FAILED") {
                context.onSyncStateChange("FAILED", syncSummary);
            }
            console.log("Sync completed with error: " + syncSummary.syncException);
        } else {
            context.onSyncStateChange("SUCCEEDED", syncSummary);
            console.log("Sync completed successfully");
        }

        return syncSummary;

    } else {
        throw new Error("There is already an active sync session. Will not start a new one.");
    }

}

/**
 * Process sync schema: retrieve from DB, compose derived data, create/migrate realm, add listener
 */

async function initSyncSchema(syncSchemaRow) {
    console.log("begin initSyncSchema for " + syncSchemaRow["SYNC_SCHEMA_NAME"]);

    // retrieve from DB

    console.log("retrieve from DB");
    let syncSchema = {};
    let syncSchemaSub = {};

    syncSchema.id = syncSchemaRow["SYNC_SCHEMA_ID"];
    syncSchemaSub.syncSchemaId = syncSchema.id;
    syncSchema.name = syncSchemaRow["SYNC_SCHEMA_NAME"];
    syncSchema.clientDbSchema = syncSchemaRow["CLIENT_DB_SCHEMA"];
    syncSchema.defCn = syncSchemaRow["DEF_CN"];
    syncSchemaSub.defCn = syncSchema.defCn;
    syncSchema.subCn = syncSchemaRow["SUB_CN"];
    syncSchemaSub.subCn = syncSchema.subCn;
    syncSchemaSub.dataCn = syncSchemaRow["DATA_CN"];
    syncSchemaSub.syncClientId = syncSchemaRow["SYNC_CLIENT_ID"];
    syncSchema.serverDbType = syncSchemaRow["SERVER_DB_TYPE"];
    syncSchema.noInitSyncNetworks = syncSchemaRow["NO_INIT_SYNC_NETWORKS"];
    syncSchema.noSyncNetworks = syncSchemaRow["NO_SYNC_NETWORKS"];

    let tableList = [];
    let newTablesList = [];

    let syncTableRows = pvcAdminRealm.objects("pvc__sync_tables");
    syncTableRows = syncTableRows.filtered("SYNC_SCHEMA_ID=" + syncSchema.id);
    for (let syncTableRow of syncTableRows) {
        console.log("syncTableName=" + syncTableRow["NAME"]);
        let syncTable = {};
        let tableSub = {};
        syncTable.id = syncTableRow["ID"];
        tableSub.tableId = syncTable.id;
        syncTable.defCn = syncTableRow["DEF_CN"];
        syncTable.name = syncTableRow["NAME"];
        syncTable.rank = syncTableRow["RANK"];
        syncTable.allowCheckIn =
            "Y" == (syncTableRow["ALLOW_CHECK_IN"]);
        syncTable.allowRefresh =
            "Y" == (syncTableRow["ALLOW_REFRESH"]);
        let strCheckInSuperUsers = syncTableRow["CHECK_IN_SUPER_USERS"];
        syncTable.checkInSuperUsers = strCheckInSuperUsers.split(",");
        let isNew = ("Y" == syncTableRow["IS_NEW"]);
        if (isNew) {
            newTablesList.push(syncTable.id);
        }
        syncTable.subsettingMode = (syncTableRow["SUBSETTING_MODE"]);
        syncTable.subsettingQuery = syncTableRow["SUBSETTING_QUERY"];

        tableList.push(syncTable);

        // pvc__sync_table_columns
        let realmColumns = syncTableRow["COLUMNS"];

        syncTable.columns = [];

        for (let realmColumn of realmColumns) {
            let column = {};
            column.syncTableIdColumnName = realmColumn["SYNC_TABLE_ID__NAME"];
            column.syncTableId = realmColumn["SYNC_TABLE_ID"];
            column.columnName = realmColumn["NAME"];
            column.deviceColDef = JSON.parse(realmColumn["DEVICE_COL_DEF"]);
            column.dataType = realmColumn["JDBC_TYPE"];
            column.typeName = realmColumn["NATIVE_TYPE"];
            column.columnSize = realmColumn["COLUMN_SIZE"];
            column.decimalDigits = realmColumn["SCALE"];
            column.nullable =
                ("Y" == (realmColumn["NULLABLE"]));
            column.pkSeq = realmColumn["PK_SEQ"];
            column.ordinalPosition = realmColumn["ORDINAL_POSITION"];
            syncTable.columns.push(column);
        }

    }
    syncSchema.tableList = tableList;
    syncSchemaSub.newTables = newTablesList;

    // post DB retrieval processing
    console.log("post DB retrieval processing");
    // delete from collections
    delete clientSchemaMap[syncSchema.id];
    delete clientSchemaSubMap[syncSchemaSub.syncSchemaId];
    let i = clientSchemaList.length;
    while (i--) {
        if (clientSchemaList[i].id == syncSchema.id) {
            clientSchemaList.splice(i, 1);
        }
    }
    i = clientSchemaSubList.length;
    while (i--) {
        if (clientSchemaSubList[i].syncSchemaId == syncSchema.id) {
            clientSchemaSubList.splice(i, 1);
        }
    }

    // insert into collections
    schemaNameIdMap[syncSchema.name] = syncSchema.id;
    clientSchemaList.push(syncSchema);
    clientSchemaMap[syncSchema.id] = syncSchema;
    clientSchemaSubList.push(syncSchemaSub);
    console.log("Pushed to clientSchemaSubList, syncSchemaSub.syncSchemaId=" + syncSchemaSub.syncSchemaId);
    clientSchemaSubMap[syncSchemaSub.syncSchemaId] = syncSchemaSub;

    // for each table, compose derived data
    console.log("for each table, compose derived data");
    syncSchema.tableMap = {};
    syncSchemaSub.tableSubMap = {};
    for (let syncTable of syncSchema.tableList) {
        let tableSub = {};
        syncTable.lobColCount = 0;
        tableSub.tableId = syncTable.id;
        syncSchema.tableMap[syncTable.id] = syncTable;
        syncSchemaSub.tableSubMap[tableSub.tableId] = tableSub;


        // columns
        let colList = [];
        let pkColList = [];
        let regColList = [];
        let lobColList = [];
        syncTable.pkList = [];

        // for each column
        for (let column of syncTable.columns) {

            if (column.pkSeq > 0) {
                pkColList.push(column);
            } else if (!db.isBlob(syncSchema.serverDbType, column) &&
                !db.isClob(syncSchema.serverDbType, column)) {
                regColList.push(column);
            } else {
                lobColList.push(column);
            }
            colList.push(column);
        }
        // sort col list
        colList.sort(function (o1, o2) {
            return o1.ordinalPosition - o2.ordinalPosition
        });

        // sort pk list
        pkColList.sort(function (o1, o2) {
            return o1.pkSeq - o2.pkSeq
        });

        let pks = "";
        for (let column of pkColList) {
            if (pks.length > 0) {
                pks += "__";
            }
            syncTable.pkList.push(column.columnName);
            pks += column.columnName;
        }
        syncTable.pks = pks;

        // lob cols
        syncTable.lobColList = lobColList;
        syncTable.lobColCount = syncTable.lobColList.length;

        syncTable.colList = colList;
        syncTable.columnsPkRegLob = pkColList.concat(regColList).concat(lobColList);
    }

    // compose realmDef
    console.log("compose realmDef");
    let path = context.settings.path + "/db/" + syncSchema.clientDbSchema + ".realm";
    let encryptionKey = context.settings.encryptionKey;
    let migrationFunction = () => { };
    let realmDef = {
        path: path,
        schema: [],
        schemaVersion: syncSchema.defCn,
        migration: migrationFunction
    }

    if (encryptionKey) {
        realmDef.encryptionKey = encryptionKey;
    }

    // compose schema tableDef
    for (let syncTable of syncSchema.tableList) {
        // compose tableDef
        let tableDef = {};
        tableDef.name = syncTable.name;
        tableDef.properties = {};
        if (syncTable.pks) {
            tableDef.primaryKey = syncTable.pks;
            if (syncTable.pkList.length > 1) {
                tableDef.properties[syncTable.pks] = 'string';
            }
        }
        for (let column of syncTable.columns) {
            tableDef.properties[column.columnName] = column.deviceColDef;
        }
        tableDef.properties["NID__"] = { type: 'string', optional: true };
        //console.log("tableDef=" + JSON.stringify(tableDef, null, 4));

        // compose mTableDef
        let mTableDef = {};
        mTableDef.name = syncTable.name + "__m";
        mTableDef.properties = {};
        if (syncTable.pks) {
            mTableDef.primaryKey = syncTable.pks;
            if (syncTable.pkList.length > 1) {
                mTableDef.properties[syncTable.pks] = 'string';
            } else {
                for (let column of syncTable.columns) {
                    if (column.pkSeq > 0) {
                        mTableDef.properties[column.columnName] = column.deviceColDef;
                        break;
                    }
                }
            }
        }
        mTableDef.properties["VERSION__"] = { type: 'int', default: -1 };
        mTableDef.properties["DML__"] = { type: 'string', optional: true, indexed: true };
        mTableDef.properties["TXN__"] = { type: 'int', optional: true, indexed: true };
        //console.log("mTableDef=" + JSON.stringify(mTableDef, null, 4));

        // push to schema
        realmDef.schema.push(tableDef);
        realmDef.schema.push(mTableDef);
    }

    // open schema realm; add change listeners
    console.log("open realm; add change listeners");
    if (realmMap[syncSchema.id]) {
        realmMap[syncSchema.id].close();
        delete realmMap[syncSchema.id];
    }
    await Realm.open(realmDef).then((realm) => {

        realmMap[syncSchema.id] = realm;

        // for each table, Add listeners
        for (let syncTable of syncSchema.tableList) {
            //console.log("Add change listener for table " + syncTable.name);

            let tableListener = (tableRows, changes) => {
                let mTableName = syncTable.name + "__m";

                let pkDeletions = [];
                let pkModifications = [];
                let pkInsertions = [];
                let nidDeletions = [];
                /* deletions are useless as rows are not in tableRows; tableRows is collection after deletions; use calculate
                for (let index of changes.deletions) {
                    let tableRow = tableRows[index];
                    //console.log("index=" + index + ", tableRow=" + JSON.stringify(tableRow, null, 4));
                    //console.log("deletions, tableRow[syncTable.pks]=" + tableRow[syncTable.pks]);

                    // find nid value
                    if (tableRow) {
                        let nidStr = tableRow["NID__"];
                        if (nidList.indexOf(nidStr) > -1) {
                            nidDeletions.push(nidStr);
                            console.log("Skip notifiction as nidStr=" + nidStr);
                            continue;
                        }

                        // find pk value
                        let pks = tableRow[syncTable.pks];
                        if (!pks) {
                            console.log("Skip notifiction as pks=" + pks);
                            continue;
                        }
                        pkDeletions.push(pks);
                    }
                }*/
                for (let index of changes.insertions) {
                    let tableRow = tableRows[index];
                    //console.log("index=" + index + ", tableRow=" + JSON.stringify(tableRow, null, 4));
                    //console.log("insertions, tableRow[" + syncTable.pks + "]=" + tableRow[syncTable.pks]);

                    // find nid value
                    let nidStr = tableRow["NID__"];
                    if (nidList.indexOf(nidStr) > -1) {
                        nidDeletions.push(nidStr);
                        //console.log("Skip notifiction as nidStr=" + nidStr);
                        continue;
                    }

                    // find pk value
                    let pks = tableRow[syncTable.pks];
                    if (!pks) {
                        console.log("Skip notifiction as pks=" + pks);
                        continue;
                    }
                    pkInsertions.push(pks);
                }
                for (let index of changes.modifications) {
                    let tableRow = tableRows[index];
                    //console.log("index=" + index + ", tableRow=" + JSON.stringify(tableRow, null, 4));
                    //console.log("modifications, tableRow[syncTable.pks]=" + tableRow[syncTable.pks]);

                    // find nid value
                    let nidStr = tableRow["NID__"];
                    if (nidList.indexOf(nidStr) > -1) {
                        nidDeletions.push(nidStr);
                        //console.log("Skip notifiction as nidStr=" + nidStr);
                        continue;
                    }

                    // find pk value

                    let pks = tableRow[syncTable.pks];
                    if (!pks) {
                        console.log("Skip notifiction as pks=" + pks);
                        continue;
                    }
                    pkModifications.push(pks);
                }

                // work around https://github.com/realm/realm-js/issues/1552 
                // delete causes additional delete/insert
                let pksToIgnore = [];
                for (let pks of pkInsertions) {
                    if (pkDeletions.indexOf(pks) > -1) {
                        pksToIgnore.push(pks);
                        console.log("Added to pksToIgnore, pks=" + pks);
                    }
                }

                // remove changes to ignore
                for (let pks of pksToIgnore) {
                    let index = pkDeletions.indexOf(pks);
                    if (index > -1) {
                        pkDeletions.splice([index], 1);
                        console.log("Deleted from pkDeletions, pks=" + pks);
                    }
                    index = pkInsertions.indexOf(pks);
                    if (index > -1) {
                        pkInsertions.splice([index], 1);
                        console.log("Deleted from pkInsertions, pks=" + pks);
                    }
                }

                // remove from nidList
                for (let nidStr of nidDeletions) {
                    let index = nidList.indexOf(nidStr);
                    if (index > -1) {
                        nidList.splice([index], 1);
                        console.log("Deleted from nidList, nidStr=" + nidStr);
                    }
                }

                let mTableRows = [];
                for (let pks of pkInsertions) {
                    let mTableRowNew = {};
                    mTableRowNew[syncTable.pks] = pks;
                    let mTableRow = realm.objectForPrimaryKey(mTableName, pks);
                    if (mTableRow) {
                        mTableRowNew["VERSION__"] = mTableRow["VERSION__"];
                        mTableRowNew["DML__"] = (Number(mTableRow["VERSION__"]) == -1) ? "I" : "U";
                    } else {
                        mTableRowNew["VERSION__"] = -1;
                        mTableRowNew["DML__"] = "I";
                    }
                    mTableRows.push(mTableRowNew);
                }
                for (let pks of pkDeletions) {
                    let mTableRowNew = {};
                    mTableRowNew[syncTable.pks] = pks;
                    let mTableRow = realm.objectForPrimaryKey(mTableName, pks);
                    if (mTableRow) {
                        mTableRowNew["VERSION__"] = mTableRow["VERSION__"];
                        mTableRowNew["DML__"] = "D";
                        mTableRows.push(mTableRowNew);
                    } else {
                        console.log("Error, mTableRow not found for pkDeletions, pks=" + pks);
                    }
                }
                for (let pks of pkModifications) {
                    let mTableRowNew = {};
                    mTableRowNew[syncTable.pks] = pks;
                    let mTableRow = realm.objectForPrimaryKey(mTableName, pks);
                    if (mTableRow) {
                        mTableRowNew["VERSION__"] = mTableRow["VERSION__"];
                        mTableRowNew["DML__"] = (Number(mTableRow["VERSION__"]) == -1) ? "I" : "U";
                    } else {
                        mTableRowNew["VERSION__"] = -1;
                        mTableRowNew["DML__"] = "I";
                        console.log("Error, mTableRow not found for pkModifications, pks=" + pks);
                    }
                    mTableRows.push(mTableRowNew);
                }
                //console.log("mTableRows.length=" + mTableRows.length);
                if (mTableRows.length > 0) {
                    let funWrite = () => {
                        realm.write(() => {
                            for (let mTableRow of mTableRows) {
                                console.log("realm.create, " + JSON.stringify(mTableRow));
                                realm.create(mTableName, mTableRow, true);
                            }
                        });
                    }
                    db.safeWrite(realm, funWrite);
                }
            }

            let funWrite = () => {
                realm.objects(syncTable.name).addListener(tableListener);
            }
            db.safeWrite(realm, funWrite);
        }

    });

    console.log("end initSyncSchema for " + syncSchema.name);
}

/**
 * Process sync Folder: retrieve from DB, compose derived data, create/migrate realm, add listener
 */

async function initSyncFolder(syncFolderRow) {
    console.log("begin initSyncFolder for " + syncFolderRow["SYNC_FOLDER_NAME"]);

    // retrieve from DB

    console.log("retrieve from DB");
    let syncFolder = {};
    let syncFolderSub = {};

    syncFolder.id = syncFolderRow["ID"];
    syncFolderSub.syncFolderId = syncFolder.id;
    syncFolder.name = syncFolderRow["SYNC_FOLDER_NAME"];
    syncFolder.serverFolderPath = syncFolderRow["SERVER_FOLDER_PATH"];
    syncFolder.clientFolderPath = syncFolderRow["CLIENT_FOLDER_PATH"];
    syncFolder.recursive = ("Y" == syncFolderRow["RECURSIVE"]);
    syncFolder.filePathStartsWith = syncFolderRow["FILE_PATH_STARTS_WITH"];
    syncFolder.fileNameEndsWith = syncFolderRow["FILE_NAME_ENDS_WITH"];
    syncFolder.allowCheckIn = ("Y" == syncFolderRow["ALLOW_CHECK_IN"]);
    syncFolder.allowRefresh = ("Y" == syncFolderRow["ALLOW_REFRESH"]);
    let strCheckInSuperUsers = syncFolderRow["CHECK_IN_SUPER_USERS"];
    syncFolder.checkInSuperUsers = strCheckInSuperUsers.split(",");
    syncFolder.defCn = syncFolderRow["DEF_CN"];
    syncFolderSub.defCn = syncFolder.defCn;
    syncFolder.subCn = syncFolderRow["SUB_CN"];
    syncFolderSub.subCn = syncFolder.subCn;
    syncFolderSub.fileCn = syncFolderRow["FILE_CN"];
    syncFolderSub.syncClientId = syncFolderRow["SYNC_CLIENT_ID"];
    syncFolder.noInitSyncNetworks = syncFolderRow["NO_INIT_SYNC_NETWORKS"];
    syncFolder.noSyncNetworks = syncFolderRow["NO_SYNC_NETWORKS"];

    syncFolder.fileList = [];
    syncFolder.fileMap = {};

    let syncFileRows = pvcAdminRealm.objects("pvc__sync_files");
    syncFileRows = syncFileRows.filtered("SYNC_FOLDER_ID=" + syncFolder.id);
    for (let syncFileRow of syncFileRows) {
        //console.log("syncFileName=" + syncFileRow["FILE_NAME"]);
        let syncFile = {};
        syncFile.syncFolderIdFileName = syncFileRow["SYNC_FOLDER_ID__FILE_NAME"];
        syncFile.syncFolderId = syncFolder.id;
        syncFile.fileName = syncFileRow["FILE_NAME"];
        syncFile.isDirectory =
            "Y" == (syncFileRow["IS_DIRECTORY"]);
        syncFile.length = syncFileRow["LENGTH"];
        syncFile.lastModified = syncFileRow["LAST_MODIFIED"];
        syncFile.fileCn = syncFileRow["FILE_CN"];
        syncFile.fileCt = syncFileRow["FILE_CT"];

        syncFolder.fileList.push(syncFile);
        syncFolder.fileMap[syncFile.fileName] = syncFile;
    }

    // post DB retrieval processing
    console.log("post DB retrieval processing for folder files");
    // delete from collections
    delete clientFolderMap[syncFolder.id];
    delete clientFolderSubMap[syncFolderSub.syncFolderId];
    let i = clientFolderList.length;
    while (i--) {
        if (clientFolderList[i].id == syncFolder.id) {
            clientFolderList.splice(i, 1);
        }
    }
    i = clientFolderSubList.length;
    while (i--) {
        if (clientFolderSubList[i].syncFolderId == syncFolder.id) {
            clientFolderSubList.splice(i, 1);
        }
    }

    // insert into collections
    clientFolderList.push(syncFolder);
    clientFolderMap[syncFolder.id] = syncFolder;
    clientFolderSubList.push(syncFolderSub);
    console.log("Pushed to clientFolderSubList, syncFolderSub.syncFolderId=" + syncFolderSub.syncFolderId);
    clientFolderSubMap[syncFolderSub.syncFolderId] = syncFolderSub;


    console.log("end initSyncFolder for " + syncFolder.name);
}

async function send() {

    // sync start
    context.onSyncStateChange("COMPOSING");

    // sync request
    let syncRequest = {};
    console.log("populating clientProperties");
    syncRequest.clientVersion = context.settings.VERSION;
    syncRequest.user = context.settings.syncUserName;
    syncRequest.device = context.settings.syncDeviceName;
    syncRequest.password = context.settings.syncUserPassword;
    syncRequest.serverId = syncServerId;
    syncRequest.clientId = syncClientId;
    // sync options
    syncRequest.syncDirection = syncSummary.syncDirection;
    syncRequest.syncSchemaNames = syncSummary.syncSchemaNames;
    syncRequest.syncFolderNames = syncSummary.syncFolderNames;

    //
    // Upload phase
    //
    console.log("Upload phase");
    syncSummary.uploadBeginTime = new Date().getTime();
    syncSummary.sessionId = syncSummary.uploadBeginTime;

    await transport.openOutputStream(context.settings.syncUserName + "-" + context.settings.syncDeviceName + "-"
        + syncSummary.sessionId);

    let cmd = {};
    cmd.name = "SYNC_REQUEST";
    console.log(cmd.name);
    cmd.value = syncRequest;
    await transport.writeCommand(cmd);

    cmd = {};
    cmd.name = "SCHEMA_SUB_STATE";
    console.log(cmd.name);
    cmd.value = clientSchemaSubList;
    await transport.writeCommand(cmd);

    cmd = {};
    cmd.name = "FOLDER_SUB_STATE";
    console.log(cmd.name);
    cmd.value = clientFolderSubList;
    await transport.writeCommand(cmd);

    //
    // CHECK_IN_DATA
    //
    if (syncSummary.syncDirection == "REFRESH_ONLY") {
        console.log("syncDirection=REFRESH_ONLY. Check in skipped.");
    } else {
        console.log("Checking in client transactions");
        await checkInData();
        await checkInFiles();
    }

    cmd = {};
    cmd.name = "END_SYNC_REQUEST";
    cmd.value = null;
    await transport.writeCommand(cmd);
    console.log(cmd.name);

    await transport.closeOutputStream(receive);
}

/**
 * 
 * @param {*} payload String or hex encoded string if isBinary
 * @param {*} isBinary 
 */
async function sendLob(payload, isBinary) {
    if (!payload) {
        let syncLob = {};
        syncLob.isBinary = isBinary;
        syncLob.isNull = true;
        syncLob.totalLength = 0;
        let cmd = {};
        cmd.name = "LOB";
        cmd.value = syncLob;
        await transport.writeCommand(cmd);
    } else {
        let offset = 0;
        while (offset < payload.length) {
            let syncLob = {};
            syncLob.isBinary = isBinary;
            syncLob.isNull = false;
            syncLob.totalLength = payload.length;
            if (syncLob.isBinary) {
                syncLob.totalLength = payload.length / 2;
            }
            let chunkSize = context.settings.lobBufferSize;
            if ((payload.length - offset) < context.settings.lobBufferSize) {
                chunkSize = payload.length - offset;
            }
            syncLob.txtPayload = payload.substr(offset, chunkSize);
            offset += chunkSize;
            let cmd = {};
            cmd.name = "LOB";
            cmd.value = syncLob;
            await transport.writeCommand(cmd);
        }
    }

}
/**
 * send CheckIns for each  pervasync schemaName
 */
async function checkInData() {
    console.log("begin checkInData(). clientSchemaSubList.length=" + clientSchemaSubList.length);

    let cmd = {};
    cmd.name = "CHECK_IN_DATA";
    cmd.value = null;
    await transport.writeCommand(cmd);

    // for each schema
    for (let clientSchemaSub of clientSchemaSubList) {
        let syncSchema =
            clientSchemaMap[clientSchemaSub.syncSchemaId];
        console.log("syncSchema.name=" + syncSchema.name);

        // determine if schema is on sync list
        let isOnSyncList = false;
        for (let name of syncSummary.syncSchemaNames) {
            //console.log("name=" + name);
            if (name.toUpperCase() == syncSchema.name.toUpperCase()) {
                isOnSyncList = true;
                break;
            }
        }
        if (!isOnSyncList) {
            console.log("Will skip schema " + syncSchema.name +
                " since it's not on sync list.");
            continue;
        }

        let realm = realmMap[syncSchema.id];
        if (!realm) {
            throw new Error("Faild to find realm for schema " + syncSchema.name);
        }

        console.log("Doing pre check in transaction id assignment for pervasync schema " +
            syncSchema.name);

        if (!syncSchema.tableList) {
            syncSchema.tableList = [];
        }

        // update mTable transactionId
        for (let k = 0; k < syncSchema.tableList.length; k++) {
            let syncTable = syncSchema.tableList[k];
            let isSuperUser = false;
            if (syncTable.checkInSuperUsers) {
                for (let l = 0; l < syncTable.checkInSuperUsers.length; l++) {
                    if (syncTable.checkInSuperUsers[l].toUpperCase() == context.settings.syncUserName.toUpperCase()) {
                        isSuperUser = true;
                        break;
                    }
                }
            }
            if (!syncTable.allowCheckIn && !isSuperUser) {
                console.log("Skipping check in for table " + syncTable.name +
                    " since not allowed");
                continue;
            }

            realm.write(() => {
                // Calculate deletes
                console.log("Calculating deletes");
                let mTableRows = realm.objects(syncTable.name + "__m");
                mTableRows.forEach((mTableRow) => {
                    let tableRow = realm.objectForPrimaryKey(syncTable.name, mTableRow[syncTable.pks]);
                    if (!tableRow) {
                        mTableRow["DML__"] = "D";
                    }
                });

                // SET TXN__=?, DML__=(CASE WHEN VERSION__=-1 AND DML__='U' THEN 'I' "
                // "WHEN VERSION__>-1 AND DML__='I' THEN 'U' ELSE DML__ END), 
                // VERSION__=VERSION__+1 WHERE DML__ IS NOT NULL";   

                mTableRows = realm.objects(syncTable.name + "__m").filtered("DML__!=null");
                mTableRows.forEach((mTableRow) => {
                    console.log("update m table: " + syncTable.name);

                    if (mTableRow["VERSION__"] > -1 && mTableRow["DML__"] == "I") {
                        mTableRow["DML__"] = "U";
                    }
                    if (mTableRow["VERSION__"] == -1 && mTableRow["DML__"] == "U") {
                        mTableRow["DML__"] = "I";
                    }

                    mTableRow["VERSION__"] = mTableRow["VERSION__"] + 1;
                    mTableRow["TXN__"] = transactionId;
                })
            })
        }

        let cmd = {};
        cmd.name = "SCHEMA";
        cmd.value = clientSchemaSub;
        await transport.writeCommand(cmd);

        // Table iterator
        let tableList = syncSchema.tableList;
        //let dmlType = ["D", "I", "U"];
        for (let dml = 0; dml < 3; dml++) {
            if (dml > 0) {
                tableList.reverse();
            }

            for (let k = 0; k < tableList.length; k++) {
                let syncTable = tableList[k];

                let mTableRows = realm.objects(syncTable.name + "__m");
                if (dml == 0) { // delete
                    // SELECT VERSION__,pks  WHERE DML__='D' AND TXN__=?";                   
                    mTableRows = mTableRows.filtered("DML__='D' AND TXN__=" + transactionId);
                } else if (dml == 1) { // Insert
                    // m.VERSION__," + tCols WHERE m.DML__=? AND TXN__=?
                    mTableRows = mTableRows.filtered("DML__='I' AND TXN__=" + transactionId);
                } else if (dml == 2) { // update
                    mTableRows = mTableRows.filtered("DML__='U' AND TXN__=" + transactionId);
                }

                if (mTableRows.length > 0) {
                    let count = 0;
                    let dmlCmd = null;
                    switch (dml) {
                        case 0:
                            dmlCmd = "DELETE";
                            break;
                        case 1:
                            dmlCmd = "INSERT";
                            break;
                        case 2:
                            dmlCmd = "UPDATE";
                            break;
                    }
                    let cmd = {};
                    cmd.name = dmlCmd;
                    cmd.value = syncTable.id;
                    await transport.writeCommand(cmd);

                    for (let mTableRow of mTableRows) {
                        let tableRow = null;
                        let pkVals = [];
                        count++;
                        if (dml == 0) {
                            syncSummary.checkInDIU_requested[0] += 1;
                        } else if (dml == 1) {
                            syncSummary.checkInDIU_requested[1] += 1;
                        } else {
                            syncSummary.checkInDIU_requested[2] += 1;
                        }
                        let colValList = [];
                        //let splitted;
                        colValList.push(String(mTableRow["VERSION__"])); // version col
                        if (dml == 0) { // delete
                            for (let m = 0; m < syncTable.pkList.length; m++) {
                                let obj = mTableRow[syncTable.columnsPkRegLob[m].columnName];

                                if (obj != null) {
                                    // cast to String
                                    let str = db.colObjToString(
                                        obj, syncSchema.serverDbType, syncTable.columnsPkRegLob[m]);
                                    colValList.push(str);
                                } else {
                                    colValList.push(null);
                                }
                            }
                        } else { // insert or update
                            tableRow = realm.objectForPrimaryKey(syncTable.name, mTableRow[syncTable.pks]);
                            for (let m = 0; m < syncTable.columnsPkRegLob.length - syncTable.lobColCount; m++) {
                                let obj = tableRow[syncTable.columnsPkRegLob[m].columnName];

                                if (obj != null) {
                                    // cast to String
                                    let str = db.colObjToString(
                                        obj, syncSchema.serverDbType, syncTable.columnsPkRegLob[m]);
                                    colValList.push(str);
                                } else {
                                    colValList.push(null);
                                }
                                if (m < syncTable.pkList.length) {
                                    pkVals.push(obj);
                                }
                            }
                        }
                        let cmd = {};
                        cmd.name = "ROW";
                        cmd.value = colValList;
                        await transport.writeCommand(cmd);

                        // lob payloads
                        if ((dml == 1 || dml == 2) && syncTable.lobColCount > 0) { // insert/update and there are  lob cols
                            for (let m = 0; m < syncTable.lobColCount; m++) {
                                let column =
                                    syncTable.columnsPkRegLob[m + syncTable.columnsPkRegLob.length - syncTable.lobColCount];
                                let isBinary = (db.isBlob(syncSchema.serverDbType, column));
                                let payload = tableRow[column.columnName];
                                if (isBinary) {
                                    // ArrayBuffer to hex encoded Uint8Array
                                    payload = new Uint8Array(payload);
                                    payload = util.bytes2hex(payload);
                                }
                                await sendLob(payload, isBinary);
                            }

                        }
                    }

                    // TABLE DML (INSERT UPDATE DELETE) "END"
                    let end_cmd =
                        "END_" + dmlCmd;
                    cmd = {};
                    cmd.name = end_cmd;
                    cmd.value = null;
                    await transport.writeCommand(cmd);

                    console.log(syncTable.name + ", " + dmlCmd + ", " +
                        count);
                }
            }
        }
        // END_SCHEMA
        cmd = {};
        cmd.name = "END_SCHEMA";
        cmd.value = null;
        await transport.writeCommand(cmd);
    }

    // END_CHECK_IN_DATA
    cmd = {};
    cmd.name = "END_CHECK_IN_DATA";
    cmd.value = null;
    await transport.writeCommand(cmd);
    console.log("end checkInData()");
}

/**
    * scanFolder called by checkInFiles() to process folder files
    *
    * @param syncFolder SyncFolder
    * @param strFolder directory path relative to syncFolder path; no starting
    * or ending file separators
    * @throws Throwable
    */
async function scanFolder(syncFolder, strFolder) {
    //console.log("scanFolder, strFolder=" + strFolder);
    let syncFolderPath = context.settings.path + "/files/" + syncFolder.clientFolderPath;
    //console.log("scanFolder, syncFolderPath=" + syncFolderPath);
    let folderPath = syncFolderPath;
    if (strFolder) {
        folderPath += "/" + strFolder;
    }
    //console.log("scanFolder, folderPath=" + folderPath);

    let folderExists = await fs.exists(folderPath);
    if (!folderExists) {
        return;
    }
    let isDir = await fs.isDir(folderPath);
    if (!isDir) {
        return;
    }

    let fileNameArray = await fs.ls(folderPath);
    for (let fileName of fileNameArray) {
        //console.log("scanFolder, fileName=" + fileName);
        let syncFilePath = fileName;
        if (strFolder) {
            syncFilePath = strFolder + "/" + fileName;
        }
        let syncFile = syncFolder.fileMap[syncFilePath];
        let fullPath = syncFolderPath + "/" + syncFilePath;
        //console.log("scanFolder, fullPath=" + fullPath);

        let isDir = await fs.isDir(fullPath);
        let stat = await fs.stat(fullPath);

        if (!isDir && stat.size == 0) {
            console.log("Ignoring empty file: " + fullPath);
            if (syncFile) {
                syncFile.exists = true; // so that file won't be marked as delete
            }
            continue;
        }

        // non-empty directory
        if (isDir) {
            let files = await fs.ls(fullPath);
            if (files.length > 0) {
                if (syncFolder.recursive) {
                    await scanFolder(syncFolder, syncFilePath);
                }
                /*if (syncFile != null) {
                    console.log("scanFolder, set dir syncFile.exists = true" );
                    syncFile.exists = true;
                }*/
            }
        }

        // file and empty dir
        if (syncFile == null) {
            console.log("Found new file. File name: " + syncFilePath);
            // insert
            syncFile = {};
            syncFile.fileName = syncFilePath;
            syncFile.isDirectory = isDir;
            syncFile.length = stat.size;
            syncFile.lastModified = stat.lastModified;
            syncFile.fileCt = "I";
            syncFile.fileCn = -1;
            syncFile.exists = true;
            syncFolder.fileMap[syncFile.fileName] = syncFile;
            syncFolder.fileList.push(syncFile);
            pvcAdminRealm.write(() => {
                let syncFileRow = {};
                syncFileRow["SYNC_FOLDER_ID__FILE_NAME"] = syncFolder.id + "__" + syncFile.fileName;
                syncFileRow["SYNC_FOLDER_ID"] = syncFolder.id;
                syncFileRow["FILE_NAME"] = syncFile.fileName;
                syncFileRow["IS_DIRECTORY"] = isDir ? "Y" : "N";
                syncFileRow["LENGTH"] = syncFile.length;
                syncFileRow["LAST_MODIFIED"] = syncFile.lastModified;
                syncFileRow["FILE_CN"] = syncFile.fileCn;
                syncFileRow["FILE_CT"] = "I"; // 'S'--Server Synced, 'I','U','D'-- Client changes
                syncFileRow["TXN__"] = transactionId;
                syncFileRow["ADDED"] = new Date();

                pvcAdminRealm.create("pvc__sync_files", syncFileRow, true);
            });
        } else {
            // mark existing file so that we can identify deleted files
            // (syncFile.exists = false)
            //console.log("scanFolder, set syncFile.exists = true for syncFilePath: " + syncFilePath);
            syncFile.exists = true;

            if (stat.lastModified / 1000 != syncFile.lastModified / 1000 || // accuracy: second
                stat.size != syncFile.length) {

                console.log("File updated. File name: " + syncFilePath);
                //console.log("stat.lastModified: " + stat.lastModified);
                //console.log("syncFile.lastModified: " + syncFile.lastModified);
                //console.log("stat.size: " + stat.size);
                //console.log("syncFile.length: " + syncFile.length);
                // update
                syncFile.length = stat.size;
                syncFile.lastModified = stat.lastModified;
                if ("S" == syncFile.fileCt
                    || "D" == syncFile.fileCt) {
                    syncFile.fileCt = "U";
                }

                pvcAdminRealm.write(() => {
                    let syncFileRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_files", syncFolder.id + "__" + syncFile.fileName);
                    syncFileRow["LENGTH"] = syncFile.length;
                    syncFileRow["LAST_MODIFIED"] = syncFile.lastModified;
                    syncFileRow["FILE_CT"] = syncFile.fileCt; // 'S'--Server Synced, 'I','U','D'-- Client changes
                    syncFileRow["TXN__"] = transactionId;
                });
            }
        }
    }
}

/**
     * send CheckIns for each pervasync folder
     */
async function checkInFiles() {
    if (clientFolderSubList.length == 0) {
        return;
    }

    console.log("Doing file check in ...");

    let cmd = {};
    cmd.name = "CHECK_IN_FILES";
    cmd.value = null;
    await transport.writeCommand(cmd);

    // check in for each folder
    for (let clientFolderSub of clientFolderSubList) {
        let syncFolder = clientFolderMap[clientFolderSub.syncFolderId];

        // determine if it's in sync list 
        let isOnSyncList = false;
        for (let name of syncSummary.syncFolderNames) {
            //console.log("name=" + name);
            if (name.toUpperCase() == syncFolder.name.toUpperCase()) {
                isOnSyncList = true;
                break;
            }
        }

        if (!isOnSyncList) {
            console.log("Skipping folder " + syncFolder.name
                + " since it's not on sync list.");
            continue;
        }


        console.log("Doing folder " + syncFolder.name);

        // No checkin until folder is first refreshed
        if (syncFolder.allowRefresh && clientFolderSub.fileCn < 0) {
            console.log("No checkin until folder is first refreshed");
            continue;
        }

        if (!syncFolder.allowCheckIn) {
            continue;
        }

        cmd = {};
        cmd.name = "FOLDER";
        cmd.value = clientFolderSub;
        transport.writeCommand(cmd);

        // process folder files

        let folderPath = null;
        folderPath = context.settings.path + "/files/" + syncFolder.clientFolderPath;

        let folderExistes = await fs.exists(folderPath);
        if (!folderExistes) {
            await fs.mkdirs(folderPath);
        }

        // Set file exists flag
        for (let syncFile of syncFolder.fileList) {
            syncFile.exists = false;
        }

        // scanSubFolder
        await scanFolder(syncFolder, "");

        // Update deleted files in DB
        for (let syncFile of syncFolder.fileList) {
            if (!syncFile.exists) {
                syncFile.fileCt = "D";

                pvcAdminRealm.write(() => {
                    let syncFileRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_files",
                        syncFolder.id + "__" + syncFile.fileName);
                    syncFileRow["FILE_CT"] = syncFile.fileCt; // 'S'--Server Synced, 'I','U','D'-- Client changes
                    syncFileRow["TXN__"] = transactionId;
                });
            }
        }

        let prefixes = syncFolder.filePathStartsWith.split(",");
        let suffixes = syncFolder.fileNameEndsWith.split(",");

        // check in folder files
        for (let syncFile of syncFolder.fileList) {
            if ("S" == syncFile.fileCt || syncFile.fileCt == null) {
                continue;
            }
            // only send file changes that satisfy the match criteria
            let prefixMatch = false;
            let suffixMatch = false;
            console.log("syncFile.fileName=" + syncFile.fileName);
            if (prefixes.length < 1) {
                prefixMatch = true;
            } else {
                prefixMatch = false;
                for (let prefix of prefixes) {
                    if (syncFile.fileName.startsWith(prefix)) {
                        prefixMatch = true;
                        console.log("matches with prefixe " + prefix);
                        break;
                    }
                }
            }

            if (suffixes.length < 1) {
                suffixMatch = true;
            } else {
                suffixMatch = false;
                for (let suffix of suffixes) {
                    if (syncFile.fileName.endsWith(suffix)) {
                        suffixMatch = true;
                        console.log("matches with suffix " + suffix);
                        break;
                    }
                }
            }

            if (prefixMatch && suffixMatch) {
                cmd = {};
                cmd.name = "FILE";
                cmd.value = syncFile;
                transport.writeCommand(cmd);

                if (!syncFile.isDirectory) {
                    if ("D" == syncFile.fileCt) {
                        syncSummary.checkInDIU_requested[0] += 1;
                        console.log("Sending delete for "
                            + syncFile.fileName);
                    } else if ("I" == syncFile.fileCt) {
                        syncSummary.checkInDIU_requested[1] += 1;
                        console.log("Sending insert for "
                            + syncFile.fileName);
                    } else {
                        syncSummary.checkInDIU_requested[2] += 1;
                        console.log("Sending update for "
                            + syncFile.fileName);
                    }
                }

                if ("D" == syncFile.fileCt || syncFile.isDirectory) {
                    continue;
                }

                // file contents
                let syncFilerPath = context.settings.path + "/files/" + syncFolder.clientFolderPath
                    + "/" + syncFile.fileName;
                console.log("syncFilerPath=" + syncFilerPath);
                let stat = await fs.stat(syncFilerPath);
                console.log("stat.size= " + stat.size);
                console.log("stat.lastModified= " + stat.lastModified);

                let bytes = await fs.readBytes(syncFilerPath);
                console.log("fs.readBytes returned bytes lenght: " + bytes.length);
                let payload = util.bytes2hex(bytes);
                console.log("util.bytes2hex returned payload lenght: " + payload.length);
                await sendLob(payload, true);
            }
        }

        // END_FOLDER
        cmd = {};
        cmd.name = "END_FOLDER";
        cmd.value = null;
        transport.writeCommand(cmd);
    }

    // END_CHECK_IN_DATA
    cmd = {};
    cmd.name = "END_CHECK_IN_FILES";
    cmd.value = null;
    transport.writeCommand(cmd);
}


async function receive() {

    context.onSyncStateChange("PROCESSING");
    syncSummary.downloadBeginTime = new Date().getTime();

    let uploadDurationInSeconds =
        (syncSummary.downloadBeginTime -
            syncSummary.uploadBeginTime) / 1000.0;
    console.log("Upload time (seconds): " +
        uploadDurationInSeconds);

    // Reading from server
    await transport.openInputStream();

    // receive SYNC_RESPONSE
    let cmd = await transport.readCommand();
    if ("SYNC_RESPONSE" != cmd.name) {
        throw Error("Expecting SYNC_RESPONSE, got " + cmd.name);
    }

    await receiveServerResponse(cmd);

    // receive END_SYNC_RESPONSE REFRESH_SCHEMA_DEF REFRESH_DATA
    syncSummary.refreshStatus = "IN_PROGRESS";
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_SYNC_RESPONSE" == cmd.name) {
            console.log("Receiving server response end (END_SYNC_RESPONSE)");
            break;
        } else {
            if ("REFRESH_SCHEMA_DEF" != cmd.name &&
                "REFRESH_FOLDER_DEF" != cmd.name &&
                "REFRESH_DATA" != cmd.name &&
                "REFRESH_FILES" != cmd.name &&
                "SYNC_SUMMARY" != cmd.name) {
                throw Error("Expecting SYNC_RESPONSE END_SYNC_RESPONSE, SYNC_SUMMARY, " +
                    "REFRESH_SCHEMA_DEF, REFRESH_FOLDER_DEF, REFRESH_FILES or REFRESH_DATA, got " + cmd.name);
            }

            if ("REFRESH_SCHEMA_DEF" == cmd.name) {
                console.log("Receiving schema definitions (REFRESH_SCHEMA_DEF)");
                await receiveRefreshSchemaDef(cmd);
            } else if ("REFRESH_FOLDER_DEF" == cmd.name) {
                console.log("Receiving folder definitions (REFRESH_FOLDER_DEF)");
                await receiveRefreshFolderDef(cmd);
            } else if ("REFRESH_DATA" == cmd.name) {
                console.log("Receiving schema data (REFRESH_DATA)");
                await receiveRefreshData(cmd);
            } else if ("REFRESH_FILES" == cmd.name) {
                console.log("Receiving folder files (REFRESH_FILES)");
                await receiveRefreshFiles(cmd);
            } else if ("SYNC_SUMMARY" == cmd.name) {
                console.log("Receiving server message (SYNC_SUMMARY)");
                receiveSyncSummary(cmd);
            }
        }
    }
    syncSummary.refreshStatus = "SUCCESS";
    await transport.closeInputStream();
}

/**
 * receive SYNC_RESPONSE command.
 */
async function receiveServerResponse(cmd) {
    console.log("Begin receiving server response (SYNC_RESPONSE)");
    let syncResponse = cmd.value;
    if (syncResponse.serverId < 0) {
        console.log("syncResponse.clientId=" + syncResponse.clientId);
        console.log("syncResponse.serverId=" + syncResponse.serverId);
        throw "Received invalid server response. Try sync again next time.";
    }

    // ClientProperties
    let serverVersion = syncResponse.serverVersion;
    console.log("serverVersion=" + serverVersion);

    // success list
    if (!syncResponse.successSchemaNames) {
        syncResponse.successSchemaNames = [];
    }
    let count = 0;
    for (let schemaName of syncResponse.successSchemaNames) {
        console.log("schemaName=" + schemaName);
        let syncSchema;
        for (let j = 0; j < clientSchemaList.length; j++) {
            let schema = clientSchemaList[j];
            console.log("schema=" + schema.name);
            if (schemaName.toUpperCase() == schema.name.toUpperCase()) {
                syncSchema = schema;
                break;
            }
        }

        console.log("Doing post check in cleanup for sync schema " + syncSchema.name);
        let realm = realmMap[syncSchema.id];

        if (!syncSchema.tableList) {
            syncSchema.tableList = [];
        }
        // Table iterator
        for (let k = 0; k < syncSchema.tableList.length; k++) {
            let syncTable = syncSchema.tableList[k];
            let isSuperUser = false;
            if (syncTable.checkInSuperUsers) {
                for (let l = 0; l < syncTable.checkInSuperUsers.length; l++) {
                    if (syncTable.checkInSuperUsers[l].toUpperCase() == context.settings.syncUserName.toUpperCase()) {
                        isSuperUser = true;
                        break;
                    }
                }
            }
            if (!syncTable.allowCheckIn && !isSuperUser) {
                continue;
            }

            realm.write(() => {
                // DELETE syncTable.name + "__m" WHERE DML__='D' AND TXN__=?
                // UPDATE syncTable.name + "__m"  SET DML__=NULL WHERE (DML__='I' OR DML__='U') AND TXN__=?
                let mTableRows = realm.objects(syncTable.name + "__m").filtered("TXN__=" + transactionId).snapshot();
                for (let mTableRow of mTableRows) {
                    if (!mTableRow) {
                        console.log("Error: mTableRow=null");
                    }
                    count++;
                    if (mTableRow["DML__"] == "D") {
                        realm.delete(mTableRow); // snapshot makes delete safe
                    } else { //"DML__='I' OR DML__='U'
                        mTableRow["DML__"] = null;
                    }
                }
            });
        }
    }

    // folders

    if (!syncResponse.successFolderNames) {
        syncResponse.successFolderNames = [];
    }

    for (let folderName of syncResponse.successFolderNames) {

        console.log("folderName=" + folderName);
        let syncFolder = null;
        for (let folder of clientFolderList) {
            if (folderName.toUpperCase() == folder.name.toUpperCase()) {
                syncFolder = folder;
                break;
            }
        }

        console.log("Doing post check in cleanup for  pervasync folder "
            + syncFolder.name);
        console.log("syncFolder.id=" + syncFolder.id);
        console.log("transactionId=" + transactionId);

        // loop backwards for search and delete
        let i = syncFolder.fileList.length;
        while (i--) {
            let syncFile = syncFolder.fileList[i];
            if (syncFile.fileCt == "D") {
                delete syncFolder.fileMap[syncFile.fileName];
                syncFolder.fileList.splice(i, 1);
            } else if (syncFile.fileCt == "I" || syncFile.fileCt == "U") {
                syncFile.fileCt = "S";
            }
        }

        pvcAdminRealm.write(() => {
            // delete
            let filter = "SYNC_FOLDER_ID=" + syncFolder.id //+ " AND TXN__=" + transactionId
                + " AND FILE_CT='D'";
            let filesToDelete = pvcAdminRealm.objects("pvc__sync_files").filtered(filter).snapshot();
            if (filesToDelete.length > 0) {
                pvcAdminRealm.delete(filesToDelete);
                count += filesToDelete.length;
            }
            console.log("after delete count=" + count);

            // update
            filter = "SYNC_FOLDER_ID=" + syncFolder.id
                + " AND (FILE_CT='I' OR FILE_CT='U')";//" AND TXN__=" + transactionId;
            let filesToUpdate = pvcAdminRealm.objects("pvc__sync_files").filtered(filter);
            for (let fileToUpdate of filesToUpdate) {
                fileToUpdate["FILE_CT"] = "S";
                count += 1;
            }
            console.log("after upddate count=" + count);
        });
    }

    if (count > 0) {
        transactionId++;
        context.pvcAdminRealm.write(() => {
            console.log("save transactionId to DB: " + transactionId);
            pvcAdminRealm.create("pvc__sync_client_properties", {
                NAME: "pervasync.transaction.id",
                VALUE: "" + transactionId
            }, true);
        });
    }

    let newSyncDeviceName = syncResponse.device;
    let newSyncClientId = syncResponse.clientId;
    let newSyncServerId = syncResponse.serverId;

    console.log("newSyncClientId=" + newSyncClientId);
    console.log("newSyncDeviceName=" + newSyncDeviceName);
    console.log("newSyncServerId=" + newSyncServerId);

    if (newSyncClientId != syncClientId) {
        context.pvcAdminRealm.write(() => {
            console.log("save newSyncClientId to DB: " + newSyncClientId);
            pvcAdminRealm.create("pvc__sync_client_properties", {
                NAME: "pervasync.client.id",
                VALUE: "" + newSyncClientId
            }, true);
        });
        console.log("SYNC_CLIENT_ID has changed. Old syncClientId = " +
            syncClientId + ", newSyncClientId = " +
            newSyncClientId);
        syncClientId = newSyncClientId;
    }
    if (newSyncDeviceName != context.settings.syncDeviceName) {
        context.pvcAdminRealm.write(() => {
            console.log("save newSyncDeviceName to DB: " + newSyncDeviceName);
            pvcAdminRealm.create("pvc__sync_client_properties", {
                NAME: "pervasync.device.name",
                VALUE: "" + newSyncDeviceName
            }, true);
        });
        console.log("syncDeviceName has changed. old syncDeviceName = " +
            context.settings.syncDeviceName + ", newSyncDeviceName = " +
            newSyncDeviceName);
        context.settings.syncDeviceName = newSyncDeviceName;
    }
    if (newSyncServerId != syncServerId) {
        context.pvcAdminRealm.write(() => {
            console.log("save newSyncServerId to DB: " + newSyncServerId);
            pvcAdminRealm.create("pvc__sync_client_properties", {
                NAME: "pervasync.server.id",
                VALUE: "" + newSyncServerId
            }, true);
        });
        console.log("syncServerId has changed. old syncServerId = " +
            syncServerId + ", newSyncServerId = " +
            newSyncServerId);
        syncServerId = newSyncServerId;
    }
    console.log("End receiving server response (SYNC_RESPONSE)");
}

function receiveSyncSummary(cmd) {
    let serverSyncSummary = cmd.value;

    //console.log("serverSyncSummary:\r\n" + JSON.stringify(serverSyncSummary, null, 4));
    //console.log("merging server syncSummary with client syncSummary");

    if (syncSummary.checkInStatus != "FAILURE") {
        syncSummary.checkInStatus = serverSyncSummary.checkInStatus;
    }
    if (serverSyncSummary.refreshStatus == "FAILURE") {
        syncSummary.refreshStatus = "FAILURE";
    }

    syncSummary.checkInSchemaNames = serverSyncSummary.checkInSchemaNames;
    syncSummary.checkInFolderNames = serverSyncSummary.checkInFolderNames;
    if (serverSyncSummary.checkInDIU_done) {
        syncSummary.checkInDIU_done = serverSyncSummary.checkInDIU_done;
    }
    if (serverSyncSummary.syncErrorMessages && serverSyncSummary.errorCode > 0
        && serverSyncSummary.errorCode != 2059) { // PVS_CHECK_IN_SKIPPED = 2059
        syncSummary.errorCode = serverSyncSummary.errorCode;
        let serverException = "PVC_SYNC_SERVER_REPORTED_ERROR:" + serverSyncSummary.syncErrorMessages;
        syncSummary.syncException = serverException;

        if (!syncSummary.syncErrorMessages) {
            syncSummary.syncErrorMessages =
                serverSyncSummary.syncErrorMessages;
        } else {
            syncSummary.syncErrorMessages += "\r\n" +
                serverSyncSummary.syncErrorMessages;
        }

        if (serverSyncSummary.syncErrorStacktraces) {
            if (!syncSummary.syncErrorStacktraces) {
                syncSummary.syncErrorStacktraces =
                    serverSyncSummary.syncErrorStacktraces;
            } else {
                syncSummary.syncErrorStacktraces += "\r\n" +
                    serverSyncSummary.syncErrorStacktraces;
            }
        }
    }

    syncSummary.serverSnapshotAge = serverSyncSummary.serverSnapshotAge;
}

async function receiveRefreshSchemaDef(cmd) {
    console.log("begin receiveRefreshSchemaDef()");
    let syncSchemas = cmd.value;
    if (!syncSchemas) {
        console.log("syncSchemas == null in receiveRefreshSchemaDef");
        return;
    }
    syncSummary.hasDefChanges = true;

    // try {
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw new Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // Update DB

    for (let syncSchema of syncSchemas) {

        console.log("Update metadata, syncSchema.name=" + syncSchema.name + ", syncSchema.id=" + syncSchema.id);
        pvcAdminRealm.write(() => {
            let clientSchemaSub = clientSchemaSubMap[syncSchema.id];

            // update  pervasync schema metadata

            if ("D" == syncSchema.defCt) {
                // delete from DB
                console.log("delete from DB, syncSchema=" + syncSchema.name);
                let schemaToDelete = pvcAdminRealm.objectForPrimaryKey("pvc__sync_schemas", syncSchema.id);
                pvcAdminRealm.delete(schemaToDelete);

                // delete from collections
                delete clientSchemaMap[syncSchema.id];
                delete clientSchemaSubMap[syncSchema.id];
                let j = clientSchemaList.length;
                while (j--) {
                    if (clientSchemaList[j].id == syncSchema.id) {
                        clientSchemaList.splice(j, 1);
                    }
                }
                j = clientSchemaSubList.length;
                while (j--) {
                    if (clientSchemaSubList[j].syncSchemaId == syncSchema.id) {
                        clientSchemaSubList.splice(j, 1);
                    }
                }

                let realm = realmMap[syncSchema.id];
                if (realm) {
                    realm.close();
                    delete realmMap[syncSchema.id];
                }

            } else {
                // insert/update
                console.log("insert/update DB, syncSchema=" + syncSchema.name);
                let realmSyncSchema = {};
                realmSyncSchema["SYNC_SCHEMA_ID"] = syncSchema.id;
                realmSyncSchema["SYNC_CLIENT_ID"] = syncClientId;
                realmSyncSchema["SYNC_SCHEMA_NAME"] = syncSchema.name;
                realmSyncSchema["SERVER_DB_TYPE"] = syncSchema.serverDbType;
                realmSyncSchema["SERVER_DB_SCHEMA"] = syncSchema.serverDbSchema;
                realmSyncSchema["CLIENT_DB_SCHEMA"] = syncSchema.clientDbSchema;
                realmSyncSchema["DEF_CN"] = syncSchema.defCn;
                realmSyncSchema["SUB_CN"] = syncSchema.subCn;
                realmSyncSchema["DATA_CN"] = clientSchemaSub ? clientSchemaSub.dataCn : -1;
                realmSyncSchema["NO_INIT_SYNC_NETWORKS"] = syncSchema.noInitSyncNetworks;
                realmSyncSchema["NO_SYNC_NETWORKS"] = syncSchema.noSyncNetworks;
                realmSyncSchema["ADDED"] = new Date();
                pvcAdminRealm.create("pvc__sync_schemas", realmSyncSchema, true);
            }

            // for each table, insert or delete metadata

            for (let j = 0; j < syncSchema.tableList.length; j++) {
                let syncTable = syncSchema.tableList[j];

                console.log("delete/update syncTable metadata, id=" + syncTable.id
                    + ", name=" + syncTable.name);

                // delete syncTable
                let tablesToDelete = pvcAdminRealm.objects("pvc__sync_tables").filtered("SYNC_SCHEMA_ID=" + syncSchema.id + " AND NAME='" + syncTable.name + "'");
                for (let tableToDelete of tablesToDelete) {
                    pvcAdminRealm.delete(tableToDelete);
                }

                if ("D" == syncSchema.defCt ||
                    "D" == syncTable.defCt) {
                    continue;
                }

                let realmSyncTable = {};
                realmSyncTable["ID"] = syncTable.id;
                realmSyncTable["SYNC_SCHEMA_ID"] = syncSchema.id;
                realmSyncTable["NAME"] = syncTable.name;
                realmSyncTable["RANK"] = syncTable.rank;
                realmSyncTable["DEF_CN"] = syncTable.defCn;
                realmSyncTable["DEF_CT"] = syncTable.defCt;
                realmSyncTable["SUBSETTING_MODE"] = syncTable.subsettingMode;
                realmSyncTable["SUBSETTING_QUERY"] = syncTable.subsettingQuery;
                realmSyncTable["IS_NEW"] = "Y";
                realmSyncTable["ALLOW_CHECK_IN"] = syncTable.allowCheckIn ? "Y" : "N";
                realmSyncTable["ALLOW_REFRESH"] = syncTable.allowRefresh ? "Y" : "N";
                realmSyncTable["CHECK_IN_SUPER_USERS"] = syncTable.checkInSuperUsers.join(",");
                realmSyncTable["ADDED"] = new Date();

                realmSyncTable["COLUMNS"] = [];

                for (let column of syncTable.columns) {
                    let realmColumn = {};
                    realmColumn["SYNC_TABLE_ID__NAME"] = String(syncTable.id) + "__" + column.columnName;
                    realmColumn["SYNC_TABLE_ID"] = syncTable.id;
                    realmColumn["NAME"] = column.columnName;
                    //console.log("column.columnName=" + column.columnName);
                    let deviceColDef = db.getDeviceColDef(syncSchema.serverDbType, column);
                    let deviceColDefStr = JSON.stringify(deviceColDef, null, 4);
                    realmColumn["DEVICE_COL_DEF"] = deviceColDefStr;
                    realmColumn["JDBC_TYPE"] = column.dataType;
                    realmColumn["NATIVE_TYPE"] = column.typeName;
                    realmColumn["COLUMN_SIZE"] = column.columnSize;
                    realmColumn["SCALE"] = column.decimalDigits;
                    realmColumn["NULLABLE"] = column.nullable ? "Y" : "N";
                    realmColumn["PK_SEQ"] = column.pkSeq;
                    realmColumn["ORDINAL_POSITION"] = column.ordinalPosition;
                    realmColumn["ADDED"] = new Date();
                    realmSyncTable["COLUMNS"].push(realmColumn);
                    //console.log("realmSyncTable pushed realmColumn: " + realmColumn["NAME"]);
                }

                // insert syncTable
                pvcAdminRealm.create("pvc__sync_tables", realmSyncTable, true);
            }
        });

        // retrieve from DB and init
        let syncSchemaRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_schemas", syncSchema.id);
        await initSyncSchema(syncSchemaRow);
    }

    console.log("end receiveRefreshSchemaDef()");
}

async function receiveRefreshFolderDef(cmd) {
    console.log("begin receiveRefreshFolderDef()");
    let syncFolders = cmd.value;
    if (!syncFolders) {
        console.log("syncFolders == null in receiveRefreshFolderDef");
        return;
    }
    syncSummary.hasDefChanges = true;

    // try {
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw new Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // Update DB

    for (let syncFolder of syncFolders) {

        console.log("Update metadata, syncFolder.name=" + syncFolder.name + ", syncFolder.id=" + syncFolder.id);

        // Will remove folder that has path or filter changes.
        let clientFolder = clientFolderMap[syncFolder.id];
        let clientFolderSub = clientFolderSubMap[syncFolder.id];
        if ("D" != syncFolder.defCt
            && clientFolderSub
            && clientFolder
            && (syncFolder.serverFolderPath != clientFolder.serverFolderPath
                || syncFolder.clientFolderPath != clientFolder.clientFolderPath
                || syncFolder.filePathStartsWith != clientFolder.filePathStartsWith
                || syncFolder.fileNameEndsWith != clientFolder.fileNameEndsWith)) {
            syncFolder.defCt = "D";
            console.log("Will remove folder that has def changes. Folder name: "
                + clientFolder.name);
        }
        pvcAdminRealm.write(() => {

            // update  pervasync Folder metadata

            if ("D" == syncFolder.defCt) {
                // delete from DB
                console.log("delete from DB, syncFolder=" + syncFolder.name);
                let filesToDelete = pvcAdminRealm.objects("pvc__sync_files").filtered("SYNC_FOLDER_ID=" + syncFolder.id).snapshot();
                pvcAdminRealm.delete(filesToDelete);
                let folderToDelete = pvcAdminRealm.objectForPrimaryKey("pvc__sync_folders", syncFolder.id);
                pvcAdminRealm.delete(folderToDelete);
                // delete from collections
                delete clientFolderMap[syncFolder.id];
                delete clientFolderSubMap[syncFolder.id];
                let j = clientFolderList.length;
                while (j--) {
                    if (clientFolderList[j].id == syncFolder.id) {
                        clientFolderList.splice(j, 1);
                    }
                }
                j = clientFolderSubList.length;
                while (j--) {
                    if (clientFolderSubList[j].syncFolderId == syncFolder.id) {
                        clientFolderSubList.splice(j, 1);
                    }
                }

            } else {
                // insert/update
                console.log("insert/update DB, syncFolder=" + syncFolder.name);
                let realmSyncFolder = {};
                realmSyncFolder["ID"] = syncFolder.id;
                realmSyncFolder["SYNC_FOLDER_NAME"] = syncFolder.name;
                realmSyncFolder["SERVER_FOLDER_PATH"] = syncFolder.serverFolderPath;
                realmSyncFolder["CLIENT_FOLDER_PATH"] = syncFolder.clientFolderPath;
                realmSyncFolder["RECURSIVE"] = syncFolder.recursive ? "Y" : "N";
                realmSyncFolder["FILE_PATH_STARTS_WITH"] = syncFolder.filePathStartsWith;
                realmSyncFolder["FILE_NAME_ENDS_WITH"] = syncFolder.fileNameEndsWith;
                realmSyncFolder["ALLOW_CHECK_IN"] = syncFolder.allowCheckIn ? "Y" : "N";
                realmSyncFolder["ALLOW_REFRESH"] = syncFolder.allowRefresh ? "Y" : "N";
                realmSyncFolder["CHECK_IN_SUPER_USERS"] = syncFolder.checkInSuperUsers.join(",");
                realmSyncFolder["DEF_CN"] = syncFolder.defCn;
                realmSyncFolder["SUB_CN"] = syncFolder.subCn;
                realmSyncFolder["FILE_CN"] = clientFolderSub ? clientFolderSub.fileCn : -1;
                realmSyncFolder["SYNC_CLIENT_ID"] = syncClientId;
                realmSyncFolder["NO_INIT_SYNC_NETWORKS"] = syncFolder.noInitSyncNetworks;
                realmSyncFolder["NO_SYNC_NETWORKS"] = syncFolder.noSyncNetworks;
                realmSyncFolder["ADDED"] = new Date();
                pvcAdminRealm.create("pvc__sync_folders", realmSyncFolder, true);
            }

        });

        // retrieve from DB and init
        let syncFolderRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_folders", syncFolder.id);
        await initSyncFolder(syncFolderRow);
    }

    console.log("end receiveRefreshFolderDef()");
}

/**
 * Refresh data
 */
async function receiveRefreshData(cmd) {
    console.log("Begin receiveRefreshData");
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // receive END_REFRESH_DATA SYNC_SUMMARY SCHEMA
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_REFRESH_DATA" == cmd.name) {
            console.log("Receiving (END_REFRESH_DATA)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "SCHEMA" != cmd.name) {
            throw Error("Expecting SYNC_SUMMARY, SCHEMA, or END_REFRESH_DATA, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            console.log("Receiving server SYNC_SUMMARY");
            receiveSyncSummary(cmd);
            return;
        }

        //("SCHEMA" == cmd))
        let serverSchemaSub = cmd.value;
        let clientSchemaSub =
            clientSchemaSubMap[serverSchemaSub.syncSchemaId];
        let clientSchema = clientSchemaMap[serverSchemaSub.syncSchemaId];

        if (!clientSchema) {
            throw Error("Missing schema def info " +
                "when refreshing schema data. syncSchemaId=" +
                serverSchemaSub.syncSchemaId);
        }

        // receiveSchema
        await receiveSchema(clientSchema);

        // Update syncClientAdminUser metadata
        pvcAdminRealm.write(() => {

            //    "UPDATE pvc__sync_schemas" + " SET DATA_CN=?" +" WHERE SYNC_SCHEMA_ID=?";
            let syncSchemaRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_schemas", serverSchemaSub.syncSchemaId);
            if (!syncSchemaRow) {
                throw Error("Update pvc__sync_schemas returns 0. " +
                    "syncServerId=" +
                    syncServerId +
                    " syncSchemaId=" +
                    serverSchemaSub.syncSchemaId);
            } else {
                syncSchemaRow["DATA_CN"] = serverSchemaSub.dataCn;
                clientSchemaSub.dataCn = serverSchemaSub.dataCn;
                console.log("Updated DATA_CN for schema " +
                    serverSchemaSub.syncSchemaId + " to " +
                    serverSchemaSub.dataCn);
            }

            // update IS_NEW

            if (clientSchemaSub.newTables &&
                clientSchemaSub.newTables.length > 0) {
                for (let i = 0; i < clientSchemaSub.newTables.length; i++) {
                    let tableId = clientSchemaSub.newTables[i];
                    // "UPDATE pvc__sync_tables" + " SET IS_NEW='N'" +" WHERE ID=?";
                    let syncTableRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_tables", tableId);
                    if (syncTableRow) {
                        syncTableRow["IS_NEW"] = "N";
                    }
                }
                clientSchemaSub.newTables = [];
            }

            syncSummary.refreshSchemaNames.push(clientSchema.name);
        });
    }

    console.log("End receiveRefreshData");
}

/**
 * receiveSchema, called by receiveRefreshData
 */
async function receiveSchema(clientSchema) {

    // receive END_SCHEMA ERROR dmls(INSERT, DELETE)
    for (; ;) {
        let cmd = await transport.readCommand();
        if ("END_SCHEMA" == cmd.name) {
            console.log("Receiving (END_SCHEMA)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "DELETE" != cmd.name &&
            "INSERT" != cmd.name) {
            throw Error("Expecting END_SCHEMA, SYNC_SUMMARY, DELETE, INSERT, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            console.log("Receiving server SYNC_SUMMARY");
            receiveSyncSummary(cmd);
            return false;
        }

        let dmlType = cmd.name;
        let tableId = cmd.value;
        let syncTable = clientSchema.tableMap[tableId];
        //TableSub tableSub = (TableSub) clientSchemaSub.tableSubMap.get(tableId);
        console.log("syncTable.name = " + syncTable.name);
        let receiveSuccess = await receiveDml(clientSchema, dmlType, syncTable);
        if (!receiveSuccess) {
            return false;
        }
    }
    return true;
}

/**
 * receiveDml, called by receiveSchema
 */
async function receiveDml(clientSchema, dmlType, syncTable) {
    // update Table and table mate
    let realm = realmMap[clientSchema.id];

    try {
        realm.beginTransaction();

        // receive END_DML ERROR ROW
        let updateCount;
        for (let i = 0; ; i++) {
            if (i > 0 && (i % 500) == 0) {
                realm.commitTransaction();
                realm.beginTransaction();
            }
            //console.log("Reading cmd");
            let cmd = await transport.readCommand();
            console.log("Processing cmd.name " + cmd.name);
            if (("END_" + dmlType) == cmd.name) {
                console.log("Receiving END_" + dmlType);
                break;
            } else if ("SYNC_SUMMARY" != cmd.name &&
                "ROW" != cmd.name) {
                throw Error("Expecting END_" + dmlType +
                    ", SYNC_SUMMARY or ROW, got " + cmd.name);
            }

            if ("SYNC_SUMMARY" == cmd.name) {
                //console.log("Receiving server SYNC_SUMMARY");
                receiveSyncSummary(cmd);
                return false;
            }
            //("ROW" == cmd)) {

            let colValList = cmd.value;
            if ("DELETE" == dmlType) {

                syncSummary.refreshDIU_requested[0] += 1;
                // DELETE dqTableName + " WHERE " + pkEqQs;
                // no version for server sent delete

                let pkVal;
                if (colValList.length == 1) {
                    pkVal = db.stringToColObj(
                        colValList[0], clientSchema.serverDbType, syncTable.columnsPkRegLob[0]);
                } else {
                    pkVal = "";
                    for (let k = 0; k < colValList.length; k++) {
                        if (pkVal.length > 0) {
                            pkVal += "__";
                        }
                        pkVal += String(colValList[k]);
                    }
                }

                let tableRow = realm.objectForPrimaryKey(syncTable.name, pkVal);
                let mTableRow = realm.objectForPrimaryKey(syncTable.name + "__m", pkVal);

                //realm.write(() => {

                if (tableRow) {
                    let nidStr = "SYNC_" + new Date().getTime() + "_" + nid++;
                    nidList.push(nidStr);
                    tableRow["NID__"] = nidStr;
                    realm.delete(tableRow);
                    syncSummary.refreshDIU_done[0] += 1;
                }
                if (mTableRow) {
                    realm.delete(mTableRow);
                }
                //});

            } else if ("INSERT" == dmlType) {

                syncSummary.refreshDIU_requested[1] += 1;

                let versionArr = colValList.slice(0, 1);
                let pkArr = colValList.slice(1, syncTable.pkList.length + 1);
                let colArr = colValList.slice(1, syncTable.columnsPkRegLob.length - syncTable.lobColCount + 1);
                //console.log("syncTable.columnsPkRegLob.length: " + syncTable.columnsPkRegLob.length);
                //console.log("syncTable.lobColCount: " + syncTable.lobColCount);
                //console.log("colValList: " + colValList);
                //console.log("versionArr: " + versionArr);
                //console.log("colArr.concat(pkArr): " + colArr.concat(pkArr));

                let tableRow = {};
                let mTableRow = {};

                // pk col
                let pkVal;
                if (pkArr.length == 1) {
                    pkVal = db.stringToColObj(
                        pkArr[0], clientSchema.serverDbType, syncTable.columnsPkRegLob[0]);
                } else {
                    pkVal = "";
                    for (let k = 0; k < pkArr.length; k++) {
                        if (pkVal.length > 0) {
                            pkVal += "__";
                        }
                        pkVal += String(pkArr[k]);
                    }
                }
                tableRow[syncTable.pks] = pkVal;
                mTableRow[syncTable.pks] = pkVal;
                // SET VERSION__=?, DML__=NULL
                mTableRow["VERSION__"] = Number(versionArr[0]);
                mTableRow["DML__"] = null;

                // pk and reg cols
                for (let k = 0; k < syncTable.columnsPkRegLob.length - syncTable.lobColCount; k++) {
                    let column = syncTable.columnsPkRegLob[k];
                    tableRow[column.columnName] = db.stringToColObj(
                        colArr[k], clientSchema.serverDbType, column);
                    //if(tableRow[column.columnName] == null){
                    //    console.log("delete tableRow[column.columnName]: " + column.columnName);
                    //    delete tableRow[column.columnName];
                    //}
                }

                // lob cols
                if (syncTable.lobColCount > 0) { // insert/update and there are lob cols
                    for (let i = 0; i < syncTable.lobColCount; i++) {
                        let column = syncTable.lobColList[i];
                        console.log("Rceiving LOB col " + column.columnName);
                        // receiveLob
                        let lobStr = await receiveLob();
                        if (db.isBlob(clientSchema.serverDbType, column)) {
                            tableRow[column.columnName] = db.stringToColObj(
                                lobStr, clientSchema.serverDbType, column);
                        } else {
                            tableRow[column.columnName] = lobStr;
                        }
                    }
                }

                //realm.write(() => {
                let nidStr = "SYNC_" + new Date().getTime() + "_" + nid++;
                nidList.push(nidStr);
                tableRow["NID__"] = nidStr;
                try {
                    realm.create(syncTable.name, tableRow, true);
                } catch (error) {
                    console.log("Writing to sync table " + syncTable.name + ", error=" + error
                        + ", tableRow=" +
                        JSON.stringify(tableRow, null, 4));
                    throw error;
                }

                try {
                    realm.create(syncTable.name + "__m", mTableRow, true);
                } catch (error) {
                    console.log("Writing to m table " + syncTable.name + "__m, error=" + error
                        + ", mTableRow=" +
                        JSON.stringify(mTableRow, null, 4));
                    throw error;
                }
                updateCount = 1;
                syncSummary.refreshDIU_done[1] += updateCount;
                //});

            } else {
                throw Error("PVC_WRONG_DML_TYPE: " + dmlType);
            }
        }
        realm.commitTransaction();
    } catch (error) {
        realm.cancelTransaction();
        throw error;
    }
    return true;
}

async function receiveLob() {
    //console.log("begin receiveLob");
    let lobStr = "";
    let nWriteTotal = 0;
    for (; ;) {
        let cmd = await transport.readCommand();
        let syncLob = cmd.value;
        if (syncLob.isNull || syncLob.totalLength == 0) {
            break;
        }

        lobStr += syncLob.txtPayload;
        nWriteTotal += syncLob.isBinary ? syncLob.txtPayload.length / 2 : syncLob.txtPayload.length;

        if (syncLob.isNull || nWriteTotal >= syncLob.totalLength) {
            if (nWriteTotal != syncLob.totalLength) {
                throw Error("nWriteTotal != syncLob.totalLength, nWriteTotal=" +
                    nWriteTotal +
                    ", syncLob.totalLength=" +
                    syncLob.totalLength);
            }
            console.log("nWriteTotal=" +
                nWriteTotal +
                ", syncLob.totalLength=" +
                syncLob.totalLength);
            break;
        }

    }
    //console.log("end receiveLob");
    return lobStr;
}
/**
 * Refresh data
 */
async function receiveRefreshFiles(cmd) {
    console.log("Begin receiveRefreshFiles");
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // receive END_REFRESH_FILES SYNC_SUMMARY FOLDER
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_REFRESH_FILES" == cmd.name) {
            console.log("Receiving (END_REFRESH_FILES)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "FOLDER" != cmd.name) {
            throw Error("Expecting SYNC_SUMMARY, FOLDER, or END_REFRESH_FILES, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            console.log("Receiving server SYNC_SUMMARY");
            receiveSyncSummary(cmd);
            return;
        }

        //("FOLDER" == cmd))
        let serverFolderSub = cmd.value;
        let clientFolderSub =
            clientFolderSubMap[serverFolderSub.syncFolderId];
        let clientFolder = clientFolderMap[serverFolderSub.syncFolderId];

        if (!clientFolder) {
            throw Error("Missing Folder def info " +
                "when refreshing Folder data. syncFolderId=" +
                serverFolderSub.syncFolderId);
        }

        // receiveFolder
        await receiveFolder(clientFolder);

        // Update syncClientAdminUser metadata
        pvcAdminRealm.write(() => {

            //    "UPDATE pvc__sync_folders" + " SET FILE_CN=?" +" WHERE ID=?";
            let syncFolderRow = pvcAdminRealm.objectForPrimaryKey("pvc__sync_folders", serverFolderSub.syncFolderId);
            if (!syncFolderRow) {
                throw Error("Update pvc__sync_Folders returns 0. " +
                    "syncServerId=" +
                    syncServerId +
                    " syncFolderId=" +
                    serverFolderSub.syncFolderId);
            } else {
                syncFolderRow["FILE_CN"] = serverFolderSub.fileCn;
                clientFolderSub.fileCn = serverFolderSub.fileCn;
                console.log("Updated FILE_CN for Folder " +
                    serverFolderSub.syncFolderId + " to " +
                    serverFolderSub.fileCn);
            }

            syncSummary.refreshFolderNames.push(clientFolder.name);
        });
    }

    console.log("End receiveRefreshFiles");
}

/**
 * receiveFolder, called by receiveRefreshData
 */
async function receiveFolder(syncFolder) {

    let folderPath = context.settings.path + "/files/" + syncFolder.clientFolderPath;

    // receive END_FOLDER FILE
    for (; ;) { // for each FILE
        let cmd = await transport.readCommand();
        if ("END_FOLDER" == cmd.name) {
            console.log("Receiving (END_FOLDER)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "FILE" != cmd.name) {
            throw Error("Expecting FILE, SYNC_SUMMARY or END_FOLDER, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            console.log("Receiving server SYNC_SUMMARY");
            receiveSyncSummary(cmd);
            return false;
        }

        let syncFile = cmd.value;

        let updateMetaData = false;
        let writeFile = false;
        let filePath = folderPath + "/" + syncFile.fileName;
        let isDir = syncFile.isDirectory;
        let exists = await fs.exists(filePath);

        if (isDir) {
            // directory
            if ("D" == syncFile.fileCt && exists) {
                let files = await fs.ls(filePath);
                if (files.length == 0) {
                    console.log("Deleting " + filePath);
                    await fs.rm(filePath);
                }

            } else if (("I" == syncFile.fileCt || "U" == syncFile.fileCt) && !exists) {
                await fs.mkdirs(filePath);
            }
        } else {
            // files

            if ("D" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[0] += 1;
                console.log("delete of file " + syncFile.fileName
                    + " requested");
            } else if ("I" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[1] += 1;
                console.log("insert of file " + syncFile.fileName
                    + " requested");
            } else if ("U" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[2] += 1;
                console.log("update of file " + syncFile.fileName
                    + " requested");
            }

            if ("I" == syncFile.fileCt
                || "U" == syncFile.fileCt) {
                if (!exists) {
                    syncSummary.refreshDIU_done[1] += 1;
                } else {
                    if (isDir) {
                        await fs.rm(filePath);
                    }

                    syncSummary.refreshDIU_done[2] += 1;
                }

                // create file
                let parentPath = fs.parent(filePath);
                console.log("parentPath=" + parentPath);
                let parentExists = await fs.exists(parentPath);
                if (!parentExists) {
                    console.log("calling parent.mkdirs");
                    await fs.mkdirs(parentPath);
                } else {
                    console.log("parent exists");
                }

                console.log("Creating file " + filePath);
                if (!exists) {
                    await fs.createFile(filePath);
                }

                writeFile = true;
                updateMetaData = true;
                if ("SOFTWARE_UPDATE_SYNC_FOLDER" == syncFolder.name) {
                    writeFile = false;
                }
            } else if ("D" == syncFile.fileCt) {
                // delete file
                if (!exists) {
                    syncSummary.refreshDIU_done[0] += 0;
                } else {
                    console.log("Deleting " + filePath);
                    await fs.rm(filePath);
                    syncSummary.refreshDIU_done[0] += 1;
                }
            } else {
                throw new Error("PVC_WRONG_FILE_CHANGE_TYPE:" + syncFile.fileCt);
            }

            // file content

            if ("D" != syncFile.fileCt && !isDir) {
                let lobStr = await receiveLob();
                if (writeFile && lobStr) {
                    let bytes = util.hex2bytes(lobStr);
                    await fs.writeBytes(filePath, bytes);
                    let stat = await fs.stat(filePath);
                    console.log("syncFile.length=" + syncFile.length
                        + ", stat.size="
                        + stat.size);
                    syncFile.lastModified = stat.lastModified;
                    syncFile.length = stat.size;
                }
            } // file content

            if (updateMetaData) {

                let i = syncFolder.fileList.length;
                let found = false;
                while (i--) {
                    if (syncFile.fileName == syncFolder.fileList[i].fileName) {
                        if (syncFile.fileCt == "D") {
                            delete syncFolder.fileMap[syncFile.fileName];
                            syncFolder.fileList.splice(i, 1);
                        } else {
                            syncFile.fileCt = "S";
                            syncFolder.fileList.splice(i, 1, syncFile);
                            syncFolder.fileMap[syncFile.fileName] = syncFile;
                        }
                        found = true;
                        break;
                    }
                }
                if (!found && syncFile.fileCt != "D") {
                    syncFile.fileCt = "S";
                    syncFolder.fileList.push(syncFile);
                    syncFolder.fileMap[syncFile.fileName] = syncFile;
                }

                pvcAdminRealm.write(() => {
                    // update file meta data
                    if ("D" == syncFile.fileCt) {
                        // delete

                        let syncFileToDelete = pvcAdminRealm.objectForPrimaryKey("pvc__sync_files",
                            syncFolder.id + "__" + syncFile.fileName);
                        pvcAdminRealm.delete(syncFileToDelete);

                    } else {
                        // update

                        let syncFileRow = {};
                        syncFileRow["SYNC_FOLDER_ID__FILE_NAME"] = syncFolder.id + "__" + syncFile.fileName;
                        syncFileRow["SYNC_FOLDER_ID"] = syncFolder.id;
                        syncFileRow["FILE_NAME"] = syncFile.fileName;
                        syncFileRow["IS_DIRECTORY"] = isDir ? "Y" : "N";
                        syncFileRow["LENGTH"] = syncFile.length;
                        syncFileRow["LAST_MODIFIED"] = syncFile.lastModified;
                        syncFileRow["FILE_CN"] = syncFile.fileCn;
                        syncFileRow["FILE_CT"] = syncFile.fileCt;
                        syncFileRow["ADDED"] = new Date();
                        pvcAdminRealm.create("pvc__sync_files", syncFileRow, true);
                    }
                });
            }
        }
    } // for each FILE
    return true;
}

export default {
    init,
    destroy,
    sync,
    getRealm,
    getPath,
    clientSchemaList,
    clientFolderList,
    clientSchemaMap,
    clientFolderMap
}