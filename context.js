// Pervasync state and settings

var configured = false;
var syncing = false;
var syncState = "";
function onSyncStateChange(state, syncSummary){
  syncState = state;
  console.log("sync state: " + state);
   if (typeof this.onstatechange == 'function') {
        try {
            this.onstatechange(state, syncSummary);
        } catch (e) {
          console.warn(e);
        }
    }
}

var settings = {
  path: null,
  encryptionKey: null,
  syncServerUrl: null, // required
  syncUserName: null, // required
  syncDeviceName: "DEFAULT",
  syncUserPassword: null, // required
  adminDbName: "pvcadmin.realm",
  maxMessageSize: 2000000,
  lobBufferSize: 400000,
  morePayload: "31{\"name\":\"MORE\",\"valueLength\":0}"
}

var pvcAdminRealm;

export default {
  configured,
  syncing,
  syncState,
  settings,
  pvcAdminRealm,
  onSyncStateChange
}
