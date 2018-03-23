# react-native-sync

Two way, incremental sync between React Native realmjs database and MySQL, Oracle, MS SQL Server and PostgreSQL 

## Features

* Direct DB synchronization between on device [realmjs DB](https://realm.io/docs/javascript/latest/) and server side MySQL, Oracle, MS SQL Server and PostgreSQL databases
* Each user could subscribe to a subset of server side data
* Files can also be syned

## Demo

Check out [react-native-sync-demo](https://github.com/pervasync/react-native-sync-demo).

## Setup

This library is available on npm, install it with: `npm install --save react-native-sync` or `yarn add react-native-sync`.

## Usage

1. Import react-native-sync as RNSync:

```javascript
import RNSync from "react-native-sync";
```

2. Configure RNSync:

```javascript
var settings = {
    syncServerUrl: "http://localhost:8080/pervasync/server", // required
    syncUserName: "user_1", // required
    syncUserPassword: "welcome1", // required
};
await RNSync.config(settings);
```

3. Start a sync session:

```javascript
let syncSummary = await RNSync.sync();
```

4. Get a handle to the synced realm database and synced folder path:

```javascript
let realm = await RNSync.getRealm(syncSchemaName);
let path = await RNSync.getPath(syncFolderName);
```

## Complete Example

Check out [react-native-sync-demo](https://github.com/pervasync/react-native-sync-demo) and expecially [sync.js](https://github.com/pervasync/react-native-sync-demo/blob/master/sync.js)


