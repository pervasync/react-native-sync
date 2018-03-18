import RNFetchBlob from 'react-native-fetch-blob'

/**
 * 
 * @param {String} hex Hex encoded string
 * @return bytes (of type Uint8Array)
 */
function hex2bytes(hex) {
    //console.log("begin hex2bytes");
    if (!hex) {
        return null;
    }
    let bytes = new Uint8Array(hex.match(/[\da-f]{2}/gi).map(function (h) {
        return parseInt(h, 16)
    }));
    //console.log("end hex2bytes");
    return bytes;
}

/**
 * 
 * @param {Uint8Array} bytes 
 * @return Hex encoded string
 */
function bytes2hex(bytes) {
    if (!bytes) {
        return null;
    }
    return Array.prototype.map.call(bytes, x => ('00' + x.toString(16)).slice(-2)).join('');
}

/**
 * @param {*} bytes Uint8Array
 */
function bytesToBase64(bytes) {
    // Uint8Array to string the scalable way
    let binstr = Array.prototype.map.call(bytes, function (ch) {
        return String.fromCharCode(ch);
    }).join('');

    // btoa
    return RNFetchBlob.base64.encode(binstr);
}

function base64ToBytes(base64) {
    // atob
    let binstr = RNFetchBlob.base64.decode(base64);

    // str to binary
    let bytes = new Uint8Array(binstr.length);
    Array.prototype.forEach.call(binstr, function (ch, i) {
        bytes[i] = ch.charCodeAt(0);
    });
    return bytes;
}

export default {
    hex2bytes,
    bytes2hex,
    bytesToBase64,
    base64ToBytes
}