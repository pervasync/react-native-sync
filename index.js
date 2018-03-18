import context from './context.js'
import setup from "./setup.js";
import agent from "./agent.js";
import fs from "./fs.js";

async function config(settings, reset) {

  //console.log("pervasync index.js, settings=" + JSON.stringify(settings));
  context.settings = Object.assign(context.settings, settings);
  //console.log("pervasync index.js, context.settings=" + JSON.stringify(settings));

  settings = context.settings;
  if (!settings.syncServerUrl || !settings.syncUserName || !settings.syncUserPassword) {
    throw new Error("syncServerUrl, syncUserName and syncUserPassword are required in settings.");
  }

  if (!settings.path) {
    settings.path = fs.PervasyncDir;
  }

  await setup.setup(reset);
}

function getRealm(schemaName) {
  if (context.settings.configured) {
    return agent.getRealm(schemaName);
  } else {
    throw new Error("Call Pervasync config before getRealm");
  }
}

function getPath(folderName) {
  if (context.settings.configured) {
    return agent.getPath(folderName);
  } else {
    throw new Error("Call Pervasync config before getPath");
  }
}

async function sync() {
  console.log("context.settings.path=" + context.settings.path);

  if (context.settings.configured) {
    let syncSummary = await agent.sync();
    // PVS_WRONG_SYNC_SERVER_ID = 2025
    if (syncSummary.errorCode == 2025) {
      await setup.setup(true);
      syncSummary = await agent.sync();
    }
    return syncSummary;
  } else {
    throw new Error("Call Pervasync config before sync");
  }
}

export default {
  config,
  sync,
  getRealm,
  getPath,
  clientSchemaList: agent.clientSchemaList,
  clientFolderList: agent.clientFolderList,
  clientSchemaMap: agent.clientSchemaMap,
  clientFolderMap: agent.clientFolderMap
}
