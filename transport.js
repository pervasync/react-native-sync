import context from './context.js'

//
// transport, write to and read from temp DB table
//
let pvcAdminRealm;
let sessionId, messageId, messageSize;
let payloadOutId, payloadInId;
let requestTextAry, requestText, responseText, responseOffset;
//let agentReceive;

// transport.openOutputStream
async function openOutputStream(sessionId_in) {
    sessionId = sessionId_in; // new session
    messageId = -1;
    payloadOutId = 0;
    payloadInId = 0;
    requestText = "";
    requestTextAry = [];
    messageSize = 0;
    pvcAdminRealm = context.pvcAdminRealm;
    pvcAdminRealm.write(() => {
        console.log("truncating TABLE pvc__payload_out");
        pvcAdminRealm.delete(pvcAdminRealm.objects("pvc__payload_out"));
        console.log("truncating TABLE pvc__payload_in");
        pvcAdminRealm.delete(pvcAdminRealm.objects("pvc__payload_in"));
    });
}

// transport.closeOutputStream
async function closeOutputStream() {//agentReceive_in) {
    //agentReceive = agentReceive_in;
    if (requestTextAry.length > 0) {
        pvcAdminRealm = context.pvcAdminRealm;
        pvcAdminRealm.write(() => {
            console.log("save requestText to DB");
            pvcAdminRealm.create("pvc__payload_out", {
                ID: payloadOutId,
                PAYLOAD: requestTextAry.join("")
            }, 'modified');
        });
        requestTextAry = [];
        messageSize = 0;
    }

    // reset IDs before httpSend
    payloadOutId = 0;
    payloadInId = 0;
    messageId = -1;
    await httpSend();
}

// transport.openInputStream
async function openInputStream() {
    payloadInId = 0;
    responseText = "";
}

// transport.closeInputStream
async function closeInputStream() {
}

// writeCommand
async function writeCommand(cmd) {
    //console.log("writing " + cmd.name);
    let cmdJsonLength, strCmdJsonLength, cmdJson, cmdValueJson;

    if (cmd.value) {
        cmdValueJson = JSON.stringify(cmd.value);
        if (cmdValueJson) {
            cmd.valueLength = cmdValueJson.length;
        }
    }

    let tempValue = cmd.value;
    cmd.value = null;
    cmdJson = JSON.stringify(cmd);
    cmd.value = tempValue;

    cmdJsonLength = cmdJson.length;

    if (cmdJsonLength > 0 && cmdJsonLength < 10) {
        strCmdJsonLength = "0" + cmdJsonLength;
    } else if (cmdJsonLength < 100) {
        strCmdJsonLength = "" + cmdJsonLength;
    } else {
        throw Error("cmdJsonLength not within 1 to 99: " +
            cmdJsonLength);
    }

    let lengthToWrite = strCmdJsonLength.length + cmdJson.length;
    if (cmd.valueLength > 0) {
        lengthToWrite += cmdValueJson.length;
    }

    if (lengthToWrite > context.settings.maxMessageSize) {
        throw Error("message size limit reached with a single command");
    }

    if (cmd.name != "MORE" &&
        (messageSize + lengthToWrite) > context.settings.maxMessageSize) {

        // message size limit reached, send MORE to server
        let clientMore = {};
        clientMore.name = "MORE";
        await writeCommand(clientMore);

        pvcAdminRealm = context.pvcAdminRealm;
        pvcAdminRealm.write(() => {
            console.log("save requestText to DB");
            pvcAdminRealm.create("pvc__payload_out", {
                ID: payloadOutId,
                PAYLOAD: requestTextAry.join("")
            }, 'modified');
        });
        requestTextAry = [];
        messageSize = 0;
        payloadOutId = Number(payloadOutId) + 1;

        // write the original sync command
        await writeCommand(cmd);

    } else {
        messageSize += lengthToWrite;

        //console.log("Writing:" + strCmdJsonLength + " " + cmdJson);
        requestTextAry.push(strCmdJsonLength);
        requestTextAry.push(cmdJson);
        if (cmd.valueLength > 0) {
            //console.log("cmdValueJson.length:" + cmdValueJson.length);
            requestTextAry.push(cmdValueJson);
        }
    }
}

// readCommand
async function readCommand() {
    //console.log("begin readCommand");
    let cmd, cmdJsonLength = 0;
    let cmdJson, cmdValueJson, charArray;

    // retrieve responseText from DB
    if (!responseText || responseOffset >= responseText.length) {
        console.log("retrieve responseText from DB, payloadInId=" + payloadInId);
        pvcAdminRealm = context.pvcAdminRealm;
        responseText = pvcAdminRealm.objectForPrimaryKey("pvc__payload_in", payloadInId);
        if (responseText) {
            responseOffset = 0;
            payloadInId = Number(payloadInId) + 1;
            console.log("Retrieved responseText");
            responseText = responseText['PAYLOAD'];
        } else {
            console.warn("Retrieved responseText was empty");
        }
    }

    if (!responseText) {
        throw Error("No responseText to read");
    }

    charArray = responseText.substr(responseOffset, 2);
    responseOffset += 2;
    cmdJsonLength = parseInt(new String(charArray));
    //console.log("cmdJsonLength: " + cmdJsonLength);

    charArray = responseText.substr(responseOffset, cmdJsonLength);
    responseOffset += cmdJsonLength;
    cmdJson = charArray;
    //console.log("cmdJson: " + cmdJson);
    try {
        cmd = JSON.parse(cmdJson);
    } catch (e1) {
        console.warn("Failed to parse cmdJson: " + cmdJson);
        throw e1;
    }
    //console.log("Received: " + cmd.name);

    if (cmd.valueLength > 0) {
        charArray = responseText.substr(responseOffset, cmd.valueLength);
        responseOffset += cmd.valueLength;
        cmdValueJson = charArray;
        //console.log("cmdValueJson.length: " + cmdValueJson.length);
        try {
            cmd.value = JSON.parse(cmdValueJson);
        } catch (e1) {
            console.log("Failed to parse cmdValueJson: " + cmdValueJson);
            throw e1;
        }
    }

    // received MORE from server
    if (cmd.name == "MORE") {
        console.log("received MORE");
        cmd = await readCommand();
    }

    //console.log("end readCommand. cmd.name=" + cmd.name);
    return cmd;
}

// send client commands that were cached in temp DB table
// in one or more http requests; server responses are saved
// temp DB table
var serverMore = false;
async function httpSend() {

    messageId += 1;
    requestText = "";
    responseText = "";
    responseOffset = 0;

    // retrieve requestText from DB
    console.log("retrieve requestText from DB");
    pvcAdminRealm = context.pvcAdminRealm;
    requestText = pvcAdminRealm.objectForPrimaryKey("pvc__payload_out", payloadOutId);
    if (requestText) {
        requestText = requestText['PAYLOAD'];
    }

    payloadOutId = Number(payloadOutId) + 1;

    if (!requestText && serverMore) {
        requestText = context.settings.morePayload;
        console.log("Will send client morePayload");
    }

    if (!requestText) {
        throw Error("No more request to send");
    }

    context.onSyncStateChange("SENDING");

    let headers = {
        "Content-Type": "application/octet-stream",
        "transport-serialization": "Json",
        "session-type": "SYNC",
        "max-message-size": "" + context.settings.maxMessageSize,
        "If-Modified-Since": "Sat, 1 Jan 2005 00:00:00 GMT",
    };
    if (sessionId) {
        headers["session-id"] = sessionId;
        headers["message-id"] = "" + messageId;
    } else {
        throw new Error("Invalid sessionId: " + sessionId);
    }

    console.log("Calling fetch for message #" + messageId
        + ". headers=" + JSON.stringify(headers));
        //+ ". body=" + requestText);
    await fetch(context.settings.syncServerUrl, {
        method: "POST",
        headers: headers,
        body: requestText
    }).then((response) => {
        context.onSyncStateChange("RECEIVING");
        //console.log("response=" + JSON.stringify(response));
        if (!response.ok) {
            throw Error("Got non-OK response: " + response.status);
        }
        let responseHeaders = response.headers;

        sessionId = responseHeaders.get("session-id");
        messageId = Number(responseHeaders.get("message-id"));
        console.log("Response sessionId: " + sessionId);
        console.log("Response messageId: " + messageId);
        return response.text();
    }).then((responseText) => {
        console.log("Recieved responseText, length: " + responseText.length);
        //if (responseText.length < 2000) {
            //console.log("responseText: " + responseText);
        //}

        // save responseText to DB
        pvcAdminRealm = context.pvcAdminRealm;
        pvcAdminRealm.write(() => {
            console.log("save responseText to DB");
            pvcAdminRealm.create("pvc__payload_in", {
                ID: payloadInId,
                PAYLOAD: responseText
            }, 'modified');
        });
        payloadInId = Number(payloadInId) + 1;

        // http transport done?
        if (responseText.length >= context.settings.morePayload.length &&
            context.settings.morePayload ==
            responseText.substr(responseText.length - context.settings.morePayload.length,
                context.settings.morePayload.length)) {
            console.log("Server has more to send");
            serverMore = true;
            return httpSend();
        } else {
            console.log("http transport done");
            serverMore = false;
            //agentReceive();
        }
    })
}

export default {
    openOutputStream,
    closeOutputStream,
    openInputStream,
    closeInputStream,
    writeCommand,
    readCommand
}



