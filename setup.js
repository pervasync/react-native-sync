import Realm from 'realm'
import context from './context.js'
import fs from './fs.js'
import agent from './agent.js'

async function setup(reset) {
  context.settings.configured = false;
  console.log("begin pervasync setup.js, setup(), context.settings.path=" + context.settings.path);

  if (reset) {
    console.log("will reset");
    await agent.destroy();
    if (context.pvcAdminRealm) {
      context.pvcAdminRealm.close();
    }
    await fs.rmrf(context.settings.path);
  }

  let exists = await fs.exists(context.settings.path);
  if (!exists) {
    await fs.mkdir(context.settings.path);
  }
  let dbFolder = context.settings.path + "/db";
  exists = await fs.exists(dbFolder);
  if (!exists) {
    await fs.mkdir(dbFolder);
  }
  let filesFolder = context.settings.path + "/files";
  exists = await fs.exists(filesFolder);
  if (!exists) {
    await fs.mkdir(filesFolder);
  }
  let adminDbPath = dbFolder + "/" + context.settings.adminDbName;
  let encryptionKey = context.settings.encryptionKey;
  let schemaVersion = -1;
  try {
    if (encryptionKey) {
      schemaVersion = Realm.schemaVersion(adminDbPath, encryptionKey);
    } else {
      schemaVersion = Realm.schemaVersion(adminDbPath);
    }
  } catch (err) {
    console.log("pervasync setup.js, setup(), err=" + err);
  }

  console.log("pervasync setup.js, setup(), schemaVersion =" + schemaVersion);

  let pvc__sync_client_properties = {
    name: "pvc__sync_client_properties",
    primaryKey: "NAME",
    properties: {
      NAME: "string",
      VALUE: "string"
    }
  };

  let pvc__sync_schemas = {
    name: "pvc__sync_schemas",
    primaryKey: "SYNC_SCHEMA_ID",
    properties: {
      SYNC_SCHEMA_ID: "int",
      SYNC_CLIENT_ID: "int",
      SYNC_SCHEMA_NAME: "string",
      SERVER_DB_TYPE: "string",
      SERVER_DB_SCHEMA: "string",
      CLIENT_DB_SCHEMA: "string",
      DEF_CN: "int",
      SUB_CN: "int",
      DATA_CN: "int",
      NO_INIT_SYNC_NETWORKS: { type: "string", optional: true },
      NO_SYNC_NETWORKS: { type: "string", optional: true },
      ADDED: "date"
    }
  };

  let pvc__sync_table_columns = {
    name: "pvc__sync_table_columns",
    primaryKey: "SYNC_TABLE_ID__NAME",
    properties: {
      SYNC_TABLE_ID__NAME: "string",
      SYNC_TABLE_ID: { type: 'int', indexed: true },
      NAME: "string",
      DEVICE_COL_DEF: "string",
      JDBC_TYPE: "int",
      NATIVE_TYPE: "string",
      COLUMN_SIZE: "int",
      SCALE: "int",
      NULLABLE: "string",
      PK_SEQ: "int",
      ORDINAL_POSITION: "int",
      ADDED: "date"
    }
  };

  let pvc__sync_tables = {
    name: "pvc__sync_tables",
    primaryKey: "ID",
    properties: {
      ID: "int",
      SYNC_SCHEMA_ID: { type: 'int', indexed: true },
      NAME: "string",
      RANK: "int",
      DEF_CN: "int",
      DEF_CT: "string",
      SUBSETTING_MODE: "string",
      SUBSETTING_QUERY: "string",
      IS_NEW: "string",
      ALLOW_CHECK_IN: "string",
      ALLOW_REFRESH: "string",
      CHECK_IN_SUPER_USERS: "string",
      COLUMNS: { type: "list", objectType: "pvc__sync_table_columns" },
      ADDED: "date"
    }
  };


  let pvc__sequences = {
    name: "pvc__sequences",
    primaryKey: "PVC_COMPOSITE_PK",//SEQ_SCHEMA__SEQ_NAME
    properties: {
      PVC_COMPOSITE_PK: "string",
      SEQ_SCHEMA: { type: "string", indexed: true },
      SEQ_NAME: "string",
      START_VALUE: "int",
      MAX_VALUE: "int",
      CURRENT_VALUE: "int"
    }
  };

  let pvc__sync_files = {
    name: "pvc__sync_files",
    primaryKey: "SYNC_FOLDER_ID__FILE_NAME",
    properties: {
      SYNC_FOLDER_ID__FILE_NAME: "string",
      SYNC_FOLDER_ID: { type: "int", indexed: true },
      FILE_NAME: "string",
      IS_DIRECTORY: "string",
      LENGTH: "int",
      LAST_MODIFIED: "int",
      FILE_CN: "int",
      FILE_CT: "string",
      TXN__: { type: "int", optional: true },
      ADDED: "date"
    }
  };

  let pvc__sync_folders = {
    name: "pvc__sync_folders",
    primaryKey: "ID",
    properties: {
      ID: "int",
      SYNC_CLIENT_ID: "int",
      SYNC_FOLDER_NAME: "string",
      SERVER_FOLDER_PATH: "string",
      CLIENT_FOLDER_PATH: "string",
      RECURSIVE: "string",
      FILE_PATH_STARTS_WITH: "string",
      FILE_NAME_ENDS_WITH: "string",
      ALLOW_CHECK_IN: "string",
      ALLOW_REFRESH: "string",
      CHECK_IN_SUPER_USERS: "string",
      DEF_CN: "int",
      SUB_CN: "int",
      FILE_CN: "int",
      NO_INIT_SYNC_NETWORKS: { type: "string", optional: true },
      NO_SYNC_NETWORKS: { type: "string", optional: true },
      ADDED: "date"
    }
  };

  let pvc__lob_locators = {
    name: "pvc__lob_locators",
    primaryKey: "ID",
    properties: {
      ID: "int",
      COMMAND: "string",
      LOB_LOCATOR: "string",
      ADDED: "date"
    }
  };

  let pvc__sync_history = {
    name: "pvc__sync_history",
    primaryKey: "ID",
    properties: {
      ID: "int",
      USER_NAME: "string",
      DEVICE_NAME: "string",
      BEGIN_TIME: "date",
      SYNC_TYPE: "string",
      SYNC_DIRECTION: "string",
      DURATION: "int",
      CHECK_IN_STATUS: "string",
      CHECK_IN_DELETES: "int",
      CHECK_IN_INSERTS: "int",
      CHECK_IN_UPDATES: "int",
      REFRESH_STATUS: "string",
      REFRESH_DELETES: "int",
      REFRESH_INSERTS: "int",
      REFRESH_UPDATES: "int",
      HAS_DEF_CHANGES: "string",
      ERROR_CODE: "int",
      MESSAGES: "string"
    }
  };

  let pvc__payload_out = {
    name: "pvc__payload_out",
    primaryKey: "ID",
    properties: {
      ID: "int",
      PAYLOAD: "string"
    }
  };
  let pvc__payload_in = {
    name: "pvc__payload_in",
    primaryKey: "ID",
    properties: {
      ID: "int",
      PAYLOAD: "string"
    }
  };

  let migrationFunction = function () { };
  let realmDef = {
    path: adminDbPath,
    schema: [
      pvc__sync_client_properties,
      pvc__sync_schemas,
      pvc__sync_table_columns,
      pvc__sync_tables,
      pvc__sequences,
      pvc__sync_files,
      pvc__sync_folders,
      pvc__lob_locators,
      pvc__sync_history,
      pvc__payload_out,
      pvc__payload_in
    ],
    schemaVersion: 1,
    migration: migrationFunction
  }

  if (encryptionKey) {
    realmDef.encryptionKey = encryptionKey;
  }

  /*
  let sync_client_properties_1 = Object.assign({}, sync_client_properties);
  let migrationFunction1 = function(oldRealm, newRealm) {};
  let schema_1 = {
    path: path,
    schema: [
      pvc__sync_client_properties_1,
      pvc__sync_schemas,
      pvc__sync_table_columns,
      pvc__sync_tables,
      pvc__sequences,
      pvc__sync_files,
      pvc__sync_folders,
      pvc__sync_history
      ],
    schemaVersion: 1,
    migration: migrationFunction1
  }
  */

  /*/ migration
  let schemas = [
    schema_0//, schema_1
  ]
  if (schemaVersion > -1) {
    schemaVersion++;
    while (schemaVersion < schemas.length) {
      let migratedRealm;
      if (encryptionKey) {
        migratedRealm = new Realm(schemas[schemaVersion++], encryptionKey);
      } else {
        migratedRealm = new Realm(schemas[schemaVersion++]);
      }
      migratedRealm.close();
    }
  }
 
  if (encryptionKey) {
    realm = new Realm(schemas[schemas.length - 1], encryptionKey);
  } else {
    realm = new Realm(schemas[schemas.length - 1]);
  }*/

  /*let realm;
  
  if (encryptionKey) {
    realm = new Realm(schema, encryptionKey);
  } else {
    realm = new Realm(schema);
  }*/

  let realm = await Realm.open(realmDef);
  context.pvcAdminRealm = realm;
  await agent.init();
  context.settings.configured = true;

  console.log("end pervasync setup.js, setup()");
}

export default {
  setup
}
